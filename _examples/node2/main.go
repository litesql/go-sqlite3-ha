package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	_ "github.com/litesql/go-sqlite3-ha"
)

// You need to previously exec go run ./_examples/node1

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	db, err := sql.Open("sqlite3-ha", "file:_examples/node2/my.db?_journal=WAL&_timeout=5000&replicationURL=nats://localhost:4222&name=node2&grpcToken=secret-token&grpcInsecure=true&leaderProvider=static:localhost:5000")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec("INSERT INTO users(name) values('grpc leader redirect')")
	if err != nil {
		panic(err)
	}

	var name string
	err = db.QueryRowContext(context.Background(), "SELECT name FROM users ORDER BY rowid desc LIMIT 1").Scan(&name)
	if err != nil {
		panic(err)
	}

	fmt.Println("User:", name)
}
