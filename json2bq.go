package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	projectID  = "your-project-id"
	datasetID  = "your-dataset"
	tableID    = "your-table"
	batchSize  = 1000      // 1リクエストあたりの行数
	maxWorkers = 10        // 並行書き込みの数
)

// テキストデータの構造体
type TextRecord struct {
	ID        int64     `bigquery:"id"`
	Content   string    `bigquery:"content"`
	Timestamp time.Time `bigquery:"timestamp"`
	Category  string    `bigquery:"category"`
}

func main() {
	ctx := context.Background()
	
	// 1. クライアントの初期化
	client, err := managedwriter.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("クライアント作成エラー: %v", err)
	}
	defer client.Close()

	// 2. マネージドストリームの作成
	tablePath := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", 
		projectID, datasetID, tableID)
	
	stream, err := client.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(tablePath),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithMaxInflightRequests(5),  // 同時リクエスト数
	)
	if err != nil {
		log.Fatalf("ストリーム作成エラー: %v", err)
	}
	defer stream.Close()

	// 3. スキーマの取得
	schema, err := getTableSchema(ctx)
	if err != nil {
		log.Fatalf("スキーマ取得エラー: %v", err)
	}

	// 4. Protobufディスクリプタの作成
	descriptor, err := adapt.NormalizeDescriptor(schema)
	if err != nil {
		log.Fatalf("ディスクリプタ作成エラー: %v", err)
	}

	// 5. データ生成と書き込み
	err = writeDataInParallel(ctx, stream, descriptor, schema)
	if err != nil {
		log.Fatalf("データ書き込みエラー: %v", err)
	}

	log.Println("データ書き込み完了")
}

// テーブルスキーマを取得
func getTableSchema(ctx context.Context) (*bigquery.TableSchema, error) {
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	defer bqClient.Close()

	table := bqClient.Dataset(datasetID).Table(tableID)
	metadata, err := table.Metadata(ctx)
	if err != nil {
		// テーブルが存在しない場合は作成
		return createTableSchema(ctx, bqClient)
	}
	return metadata.Schema, nil
}

// テーブルを作成（存在しない場合）
func createTableSchema(ctx context.Context, bqClient *bigquery.Client) (*bigquery.TableSchema, error) {
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType, Required: true},
		{Name: "content", Type: bigquery.StringFieldType},
		{Name: "timestamp", Type: bigquery.TimestampFieldType},
		{Name: "category", Type: bigquery.StringFieldType},
	}

	table := bqClient.Dataset(datasetID).Table(tableID)
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: schema,
	}); err != nil {
		return nil, err
	}
	
	return &schema, nil
}

// 並列書き込み処理
func writeDataInParallel(ctx context.Context, stream *managedwriter.ManagedStream, 
	descriptor protoreflect.MessageDescriptor, schema *bigquery.TableSchema) error {
	
	var wg sync.WaitGroup
	errCh := make(chan error, maxWorkers)
	dataCh := make(chan []TextRecord, maxWorkers*2)
	
	// ワーカー起動
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for batch := range dataCh {
				if err := writeBatch(ctx, stream, descriptor, schema, batch, workerID); err != nil {
					errCh <- fmt.Errorf("ワーカー%dエラー: %v", workerID, err)
					return
				}
			}
		}(i)
	}

	// データ生成と配布
	go func() {
		defer close(dataCh)
		
		var currentBatch []TextRecord
		for i := int64(1); i <= 100000; i++ { // 10万レコードを例として
			record := TextRecord{
				ID:        i,
				Content:   fmt.Sprintf("これはサンプルテキストデータです。ID: %d", i),
				Timestamp: time.Now(),
				Category:  fmt.Sprintf("category-%d", i%10),
			}
			
			currentBatch = append(currentBatch, record)
			
			if len(currentBatch) >= batchSize {
				select {
				case dataCh <- currentBatch:
					currentBatch = nil
				case err := <-errCh:
					log.Printf("データ生成中にエラー: %v", err)
					return
				}
			}
		}
		
		// 残りのデータを送信
		if len(currentBatch) > 0 {
			dataCh <- currentBatch
		}
	}()

	// 完了待機
	go func() {
		wg.Wait()
		close(errCh)
	}()

	// エラー収集
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	
	return nil
}

// バッチ書き込み処理
func writeBatch(ctx context.Context, stream *managedwriter.ManagedStream,
	descriptor protoreflect.MessageDescriptor, schema *bigquery.TableSchema,
	records []TextRecord, workerID int) error {
	
	// レコードをProtobuf形式に変換
	var rows [][]byte
	for _, record := range records {
		rowData := map[string]interface{}{
			"id":        record.ID,
			"content":   record.Content,
			"timestamp": record.Timestamp,
			"category":  record.Category,
		}
		
		protoData, err := adapt.RowToProto(schema, descriptor, rowData)
		if err != nil {
			return fmt.Errorf("Protobuf変換エラー: %v", err)
		}
		
		serialized, err := protoData.Marshal()
		if err != nil {
			return fmt.Errorf("シリアライズエラー: %v", err)
		}
		
		rows = append(rows, serialized)
	}
	
	// バッチ書き込み
	offset := int64(0)
	if len(rows) > 0 {
		result, err := stream.AppendRows(ctx, rows, managedwriter.WithOffset(offset))
		if err != nil {
			return fmt.Errorf("AppendRowsエラー: %v", err)
		}
		
		// 書き込み結果を待機（必要に応じて非同期にすることも可能）
		_, err = result.GetResult(ctx)
		if err != nil {
			return fmt.Errorf("書き込み結果エラー: %v", err)
		}
	}
	
	log.Printf("ワーカー%d: %dレコード書き込み完了", workerID, len(records))
	return nil
}

// メモリ使用量の監視
func monitorMemory() {
	var m runtime.MemStats
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		runtime.ReadMemStats(&m)
		log.Printf("メモリ使用量: Alloc = %v MiB, TotalAlloc = %v MiB, Sys = %v MiB, NumGC = %v",
			m.Alloc/1024/1024, m.TotalAlloc/1024/1024, m.Sys/1024/1024, m.NumGC)
	}
}

type Metrics struct {
	mu          sync.Mutex
	startTime   time.Time
	totalRows   int64
	successRows int64
	failedRows  int64
}

func (m *Metrics) recordSuccess(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.successRows += int64(count)
}

func (m *Metrics) printStats() {
	duration := time.Since(m.startTime)
	rowsPerSec := float64(m.successRows) / duration.Seconds()
	
	log.Printf("処理時間: %v", duration)
	log.Printf("処理行数: %d (成功: %d, 失敗: %d)", 
		m.totalRows, m.successRows, m.failedRows)
	log.Printf("スループット: %.2f rows/sec", rowsPerSec)
}
