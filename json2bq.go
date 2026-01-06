package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"golang.org/x/sync/errgroup"
)

// 設定構造体
type Config struct {
	InputPath      string
	OutputPath     string
	ProjectID      string
	DatasetID      string
	TableID        string
	BatchSize      int
	MaxWorkers     int
	DirectLoad     bool
	ChunkSize      int
	CompressOutput bool
}

// 軽量化されたメール構造体
type LeanEmail struct {
	ID            string    `bigquery:"id"`
	Date          string    `bigquery:"date"`
	From          string    `bigquery:"from"`
	To            string    `bigquery:"to"`
	Subject       string    `bigquery:"subject"`
	Body          string    `bigquery:"body"`
	HasAttachment bool      `bigquery:"has_attachment"`
	Timestamp     time.Time `bigquery:"timestamp"`
	SizeBytes     int       `bigquery:"size_bytes"`
}

// メトリクス追跡
type Metrics struct {
	mu              sync.RWMutex
	TotalProcessed  int64
	TotalBytes      int64
	SuccessRows     int64
	FailedRows      int64
	StartTime       time.Time
	Duration        time.Duration
	MemoryUsage     runtime.MemStats
}

func main() {
	// コマンドライン引数
	config := parseFlags()
	
	// メトリクスの初期化
	metrics := &Metrics{StartTime: time.Now()}
	
	// メモリ監視開始
	go monitorMemory(metrics)
	
	// 処理実行
	var err error
	if config.DirectLoad {
		err = processDirectToBigQuery(config, metrics)
	} else {
		err = processToJSONL(config, metrics)
	}
	
	if err != nil {
		log.Fatalf("処理エラー: %v", err)
	}
	
	// 統計表示
	printMetrics(metrics)
}

func parseFlags() *Config {
	config := &Config{}
	
	flag.StringVar(&config.InputPath, "input", "input.json", "入力JSONファイルパス")
	flag.StringVar(&config.OutputPath, "output", "output.jsonl", "出力ファイルパス")
	flag.StringVar(&config.ProjectID, "project", os.Getenv("GCP_PROJECT"), "GCPプロジェクトID")
	flag.StringVar(&config.DatasetID, "dataset", "email_dataset", "BigQueryデータセット")
	flag.StringVar(&config.TableID, "table", "emails", "BigQueryテーブル")
	flag.IntVar(&config.BatchSize, "batch", 1000, "バッチサイズ")
	flag.IntVar(&config.MaxWorkers, "workers", runtime.NumCPU(), "並行ワーカー数")
	flag.BoolVar(&config.DirectLoad, "direct", false, "BigQueryに直接ロード")
	flag.IntVar(&config.ChunkSize, "chunk", 10000, "チャンクサイズ")
	flag.BoolVar(&config.CompressOutput, "compress", false, "出力を圧縮")
	
	flag.Parse()
	return config
}

// BigQueryに直接書き込む処理
func processDirectToBigQuery(config *Config, metrics *Metrics) error {
	ctx := context.Background()
	
	// BigQueryクライアント初期化
	client, err := managedwriter.NewClient(ctx, config.ProjectID)
	if err != nil {
		return fmt.Errorf("クライアント作成失敗: %v", err)
	}
	defer client.Close()
	
	// テーブルスキーマ取得
	schema, err := getOrCreateTableSchema(ctx, config)
	if err != nil {
		return fmt.Errorf("スキーマ取得失敗: %v", err)
	}
	
	// Protobufディスクリプタ作成
	descriptor, err := adapt.NormalizeDescriptor(schema)
	if err != nil {
		return fmt.Errorf("ディスクリプタ作成失敗: %v", err)
	}
	
	// マネージドストリーム作成
	tablePath := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", 
		config.ProjectID, config.DatasetID, config.TableID)
	
	stream, err := client.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(tablePath),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithMaxInflightRequests(config.MaxWorkers),
	)
	if err != nil {
		return fmt.Errorf("ストリーム作成失敗: %v", err)
	}
	defer stream.Close()
	
	// 並列処理パイプライン
	return processPipeline(ctx, config, metrics, func(batch []LeanEmail) error {
		return writeBatchToBigQuery(ctx, stream, descriptor, schema, batch, metrics)
	})
}

// JSONLファイルに書き込む処理
func processToJSONL(config *Config, metrics *Metrics) error {
	// 出力ファイル作成
	outputFile, err := os.Create(config.OutputPath)
	if err != nil {
		return fmt.Errorf("出力ファイル作成失敗: %v", err)
	}
	defer outputFile.Close()
	
	// バッファ付きライター
	writer := bufio.NewWriterSize(outputFile, 4*1024*1024) // 4MBバッファ
	defer writer.Flush()
	
	// 並列処理パイプライン
	return processPipeline(config, nil, metrics, func(batch []LeanEmail) error {
		return writeBatchToJSONL(writer, batch, metrics)
	})
}

// 共通処理パイプライン
func processPipeline(ctx context.Context, config *Config, metrics *Metrics, 
	writeFunc func([]LeanEmail) error) error {
	
	// 入力ファイルオープン
	inputFile, err := os.Open(config.InputPath)
	if err != nil {
		return fmt.Errorf("入力ファイルオープン失敗: %v", err)
	}
	defer inputFile.Close()
	
	// ワーカープール作成
	var g errgroup.Group
	emailCh := make(chan LeanEmail, config.BatchSize*config.MaxWorkers*2)
	batchCh := make(chan []LeanEmail, config.MaxWorkers)
	
	// バッチ書き込みワーカー
	for i := 0; i < config.MaxWorkers; i++ {
		workerID := i
		g.Go(func() error {
			for batch := range batchCh {
				if err := writeFunc(batch); err != nil {
					return fmt.Errorf("ワーカー%d書き込み失敗: %v", workerID, err)
				}
				metrics.mu.Lock()
				metrics.SuccessRows += int64(len(batch))
				metrics.mu.Unlock()
			}
			return nil
		})
	}
	
	// バッチ集計ワーカー
	g.Go(func() error {
		var currentBatch []LeanEmail
		for email := range emailCh {
			currentBatch = append(currentBatch, email)
			
			if len(currentBatch) >= config.BatchSize {
				select {
				case batchCh <- currentBatch:
					currentBatch = nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		
		// 残りのデータを送信
		if len(currentBatch) > 0 {
			batchCh <- currentBatch
		}
		close(batchCh)
		return nil
	})
	
	// JSON読み込みワーカー
	g.Go(func() error {
		defer close(emailCh)
		
		decoder := json.NewDecoder(inputFile)
		decoder.UseNumber() // 数値の精度保持
		
		// JSON配列またはJSONLを処理
		token, err := decoder.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("JSONトークン読み込み失敗: %v", err)
		}
		
		// JSON配列の場合
		if delim, ok := token.(json.Delim); ok && delim == '[' {
			for decoder.More() {
				var rawEmail map[string]interface{}
				if err := decoder.Decode(&rawEmail); err != nil {
					metrics.mu.Lock()
					metrics.FailedRows++
					metrics.mu.Unlock()
					continue
				}
				
				email := processEmail(rawEmail)
				select {
				case emailCh <- email:
				case <-ctx.Done():
					return ctx.Err()
				}
				
				metrics.mu.Lock()
				metrics.TotalProcessed++
				metrics.TotalBytes += int64(email.SizeBytes)
				metrics.mu.Unlock()
			}
		} else {
			// JSONLの場合（既にデコードした最初の行を処理）
			email := processEmailFromToken(token)
			emailCh <- email
			
			// 残りの行を処理
			for decoder.More() {
				var rawEmail map[string]interface{}
				if err := decoder.Decode(&rawEmail); err != nil {
					metrics.mu.Lock()
					metrics.FailedRows++
					metrics.mu.Unlock()
					continue
				}
				
				email := processEmail(rawEmail)
				select {
				case emailCh <- email:
				case <-ctx.Done():
					return ctx.Err()
				}
				
				metrics.mu.Lock()
				metrics.TotalProcessed++
				metrics.TotalBytes += int64(email.SizeBytes)
				metrics.mu.Unlock()
			}
		}
		
		return nil
	})
	
	// エラー待機
	if err := g.Wait(); err != nil {
		return err
	}
	
	metrics.Duration = time.Since(metrics.StartTime)
	return nil
}

// メールデータ処理
func processEmail(rawEmail map[string]interface{}) LeanEmail {
	email := LeanEmail{
		Timestamp: time.Now(),
	}
	
	// フィールド抽出（型安全）
	if val, ok := rawEmail["id"]; ok {
		email.ID = toString(val)
	}
	if email.ID == "" {
		email.ID = generateEmailID(rawEmail)
	}
	
	if val, ok := rawEmail["date"]; ok {
		email.Date = toString(val)
	}
	
	if val, ok := rawEmail["from"]; ok {
		email.From = truncateString(toString(val), 1000)
	}
	
	if val, ok := rawEmail["to"]; ok {
		email.To = truncateString(toString(val), 1000)
	}
	
	if val, ok := rawEmail["subject"]; ok {
		email.Subject = truncateString(toString(val), 500)
	}
	
	if val, ok := rawEmail["body"]; ok {
		email.Body = truncateString(toString(val), 2*1024*1024) // 2MB制限
	}
	
	// 添付ファイルチェック
	if val, ok := rawEmail["attachments"]; ok {
		if arr, ok := val.([]interface{}); ok && len(arr) > 0 {
			email.HasAttachment = true
		}
	}
	
	// サイズ計算
	email.SizeBytes = len(email.Body) + len(email.Subject) + len(email.From) + len(email.To)
	
	return email
}

// トークンからメール処理（JSONL用）
func processEmailFromToken(token json.Token) LeanEmail {
	// 簡易実装、実際は適切な変換が必要
	return LeanEmail{
		ID:        fmt.Sprintf("email_%d", time.Now().UnixNano()),
		Timestamp: time.Now(),
	}
}

// BigQueryバッチ書き込み
func writeBatchToBigQuery(ctx context.Context, stream *managedwriter.ManagedStream,
	descriptor protoreflect.MessageDescriptor, schema *bigquery.TableSchema,
	batch []LeanEmail, metrics *Metrics) error {
	
	var rows [][]byte
	for _, email := range batch {
		rowData := map[string]interface{}{
			"id":             email.ID,
			"date":           email.Date,
			"from":           email.From,
			"to":             email.To,
			"subject":        email.Subject,
			"body":           email.Body,
			"has_attachment": email.HasAttachment,
			"timestamp":      email.Timestamp,
			"size_bytes":     email.SizeBytes,
		}
		
		protoData, err := adapt.RowToProto(schema, descriptor, rowData)
		if err != nil {
			metrics.mu.Lock()
			metrics.FailedRows++
			metrics.mu.Unlock()
			continue
		}
		
		serialized, err := protoData.Marshal()
		if err != nil {
			metrics.mu.Lock()
			metrics.FailedRows++
			metrics.mu.Unlock()
			continue
		}
		
		rows = append(rows, serialized)
	}
	
	if len(rows) == 0 {
		return nil
	}
	
	// 非同期書き込み
	result, err := stream.AppendRows(ctx, rows)
	if err != nil {
		return fmt.Errorf("AppendRows失敗: %v", err)
	}
	
	// 結果確認（非同期でも可）
	_, err = result.GetResult(ctx)
	if err != nil {
		return fmt.Errorf("書き込み結果失敗: %v", err)
	}
	
	return nil
}

// JSONLバッチ書き込み
func writeBatchToJSONL(writer *bufio.Writer, batch []LeanEmail, metrics *Metrics) error {
	for _, email := range batch {
		jsonBytes, err := json.Marshal(email)
		if err != nil {
			metrics.mu.Lock()
			metrics.FailedRows++
			metrics.mu.Unlock()
			continue
		}
		
		if _, err := writer.Write(jsonBytes); err != nil {
			return fmt.Errorf("JSONL書き込み失敗: %v", err)
		}
		
		if _, err := writer.WriteString("\n"); err != nil {
			return fmt.Errorf("改行書き込み失敗: %v", err)
		}
	}
	return nil
}

// テーブルスキーマの取得または作成
func getOrCreateTableSchema(ctx context.Context, config *Config) (*bigquery.TableSchema, error) {
	bqClient, err := bigquery.NewClient(ctx, config.ProjectID)
	if err != nil {
		return nil, err
	}
	defer bqClient.Close()
	
	table := bqClient.Dataset(config.DatasetID).Table(config.TableID)
	metadata, err := table.Metadata(ctx)
	if err != nil {
		// テーブルが存在しない場合は作成
		return createTable(ctx, bqClient, config)
	}
	return metadata.Schema, nil
}

// テーブル作成
func createTable(ctx context.Context, bqClient *bigquery.Client, config *Config) (*bigquery.TableSchema, error) {
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.StringFieldType, Required: true},
		{Name: "date", Type: bigquery.StringFieldType},
		{Name: "from", Type: bigquery.StringFieldType},
		{Name: "to", Type: bigquery.StringFieldType},
		{Name: "subject", Type: bigquery.StringFieldType},
		{Name: "body", Type: bigquery.StringFieldType},
		{Name: "has_attachment", Type: bigquery.BooleanFieldType},
		{Name: "timestamp", Type: bigquery.TimestampFieldType},
		{Name: "size_bytes", Type: bigquery.IntegerFieldType},
	}
	
	table := bqClient.Dataset(config.DatasetID).Table(config.TableID)
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: schema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type:  bigquery.DayPartitioningType,
			Field: "timestamp",
		},
		Clustering: &bigquery.Clustering{
			Fields: []string{"from", "has_attachment"},
		},
	}); err != nil {
		return nil, err
	}
	
	return &schema, nil
}

// ユーティリティ関数
func toString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	case float64:
		return fmt.Sprintf("%f", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func truncateString(s string, maxLength int) string {
	if len(s) <= maxLength {
		return s
	}
	return s[:maxLength]
}

func generateEmailID(email map[string]interface{}) string {
	if val, ok := email["message_id"]; ok {
		if id := toString(val); id != "" {
			return id
		}
	}
	return fmt.Sprintf("email_%d_%d", time.Now().UnixNano(), len(email))
}

// メモリ監視
func monitorMemory(metrics *Metrics) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		runtime.ReadMemStats(&metrics.MemoryUsage)
		log.Printf("メモリ: Alloc=%vMB, TotalAlloc=%vMB, Sys=%vMB, NumGC=%v",
			metrics.MemoryUsage.Alloc/1024/1024,
			metrics.MemoryUsage.TotalAlloc/1024/1024,
			metrics.MemoryUsage.Sys/1024/1024,
			metrics.MemoryUsage.NumGC)
	}
}

// メトリクス表示
func printMetrics(metrics *Metrics) {
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()
	
	fmt.Println("\n=== 処理完了 ===")
	fmt.Printf("処理時間: %v\n", metrics.Duration)
	fmt.Printf("総処理行数: %d\n", metrics.TotalProcessed)
	fmt.Printf("成功行数: %d\n", metrics.SuccessRows)
	fmt.Printf("失敗行数: %d\n", metrics.FailedRows)
	fmt.Printf("処理データ量: %.2f MB\n", float64(metrics.TotalBytes)/1024/1024)
	fmt.Printf("処理速度: %.2f 行/秒\n", 
		float64(metrics.SuccessRows)/metrics.Duration.Seconds())
	fmt.Printf("データ速度: %.2f MB/秒\n", 
		float64(metrics.TotalBytes)/1024/1024/metrics.Duration.Seconds())
	
	if metrics.FailedRows > 0 {
		fmt.Printf("成功率: %.2f%%\n", 
			float64(metrics.SuccessRows)/float64(metrics.SuccessRows+metrics.FailedRows)*100)
	}
}
