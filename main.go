package main

import (
    "context"
    "fmt"
    "log"

    "github.com/segmentio/kafka-go"
)

func main() {
    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers:   []string{"localhost:9092"},
        Topic:     "heartbeat",
        Partition: 0,
        MinBytes:  1,
        MaxBytes:  10e6,
    })
    r.SetOffset(0)

    for {
        m, err := r.ReadMessage(context.Background())
        if err != nil {
            break
        }
        fmt.Printf("message at offset %2d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
    }

    if err := r.Close(); err != nil {
        log.Fatal("failed to close reader:", err)
    }

}
