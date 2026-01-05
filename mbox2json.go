package main

import (
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/mail"
    "os"
    "strings"

    "github.com/emersion/go-mbox"
)

func main() {
    if len(os.Args) != 3 {
        fmt.Println("Usage: go run main.go input.mbox output.json")
        os.Exit(1)
    }

    inputFile := os.Args[1]
    outputFile := os.Args[2]

    file, err := os.Open(inputFile)
    if err != nil {
        log.Fatalf("入力ファイルを開けません: %v", err)
    }
    defer file.Close()

    reader := mbox.NewReader(file)

    out, err := os.Create(outputFile)
    if err != nil {
        log.Fatalf("出力ファイルを作成できません: %v", err)
    }
    defer out.Close()

    enc := json.NewEncoder(out)
    enc.SetIndent("", "  ")

    fmt.Fprintln(out, "[")
    first := true
    count := 0

    for {
        msgReader, err := reader.NextMessage()
        if err == io.EOF {
            break
        }
        if err != nil {
            log.Printf("メッセージ読み込みエラー（スキップ）: %v", err)
            continue
        }

        // メッセージ全体を読み取る
        msgBytes, err := io.ReadAll(msgReader)
        if err != nil {
            log.Printf("メッセージ読み込みエラー（スキップ）: %v", err)
            continue
        }

        // mail.ReadMessage でヘッダーとボディを分離
        msg, err := mail.ReadMessage(strings.NewReader(string(msgBytes)))
        if err != nil {
            log.Printf("メッセージ解析エラー（スキップ）: %v", err)
            continue
        }

        // ヘッダーをマップに変換
        headers := make(map[string][]string)
        for key, values := range msg.Header {
            headers[key] = values
        }

        // ボディを読み取る
        body, err := io.ReadAll(msg.Body)
        if err != nil {
            body = []byte("<body read error>")
        }

        email := map[string]interface{}{
            "headers": headers,
            "body":    string(body),
        }

        if !first {
            fmt.Fprint(out, ",")
        }
        first = false

        if err := enc.Encode(email); err != nil {
            log.Printf("JSON書き込みエラー: %v", err)
        }

        count++
        if count%100 == 0 {
            fmt.Printf("処理中... %d 件\n", count)
        }
    }

    fmt.Fprintln(out, "]")
    fmt.Printf("完了: %d 件を %s に保存\n", count, outputFile)
}
