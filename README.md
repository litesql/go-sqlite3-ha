# go-sqlite3-ha

Go database/sql driver based on [github.com/mattn/go-sqlite3](https://github.com/mattn/go-sqlite3), providing high availability for SQLite databases.

## Features

- Built on the robust foundation of `go-sqlite3`.
- High availability support for SQLite databases.
- Replication: Synchronize data across nodes using NATS.
- Customize the replication strategy
- Leaderless clusters: Read/Write from/to any node. **Last-writer wins** by default, but you can customize conflict resolutions by implementing *ChangeSetInterceptor*.
- Embedded or External NATS: Choose between an embedded NATS server or an external one for replication.
- Easy to integrate with existing Go projects.

## Installation

```bash
go get github.com/litesql/go-sqlite3-ha
```

## Usage

```go
package main

import (
    "database/sql"
    _ "github.com/litesql/go-sqlite3-ha"
)

func main() {
    db, err := sql.Open("sqlite3-ha", "file:example.db?_journal=WAL&_timeout=5000&replicationURL=nats://broker:4222&name=node0")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Use db to interact with your database
}
```

### Using Connector

```go
package main

import (
    "database/sql"
    sqlite3ha "github.com/litesql/go-sqlite3-ha"
)

func main() {
    c, err := sqlite3.NewConnector("file:example.db?_journal=WAL&_timeout=5000",
		ha.WithName("node1"),
		ha.WithEmbeddedNatsConfig(&ha.EmbeddedNatsConfig{
			Port: 4222,
		}))
	if err != nil {
		panic(err)
	}
	defer c.Close()

	db := sql.OpenDB(c)
	defer db.Close()

    // Use db to interact with your database
}
```

## Configuration

| Connector option | DSN param | Description | Default |
|------------------|-----------|-------------|---------|
| WithName | name   | Unique node name | $HOSTNAME |
| WithReplicationURL| replicationURL | NATS connection URL. (nats://localhost:4222) | |
| WithEmbeddedNatsConfig | <ul><li>natsPort</li><li>natsStoreDir</li><li>natsConfigFile</li></ul> | NATS embedded server config |
| WithDisableDDLSync | disableDDLSync| Disable replication of DDL commands | | 
| WithPublisherTimeout | publisherTimeout | Publisher timeout | 15s |
| WithChangeSetInterceptor | | Customize the replication behaviour |
| WithExtensions | | SQLite extensions to load | |

- Check out all options at [option.go](https://github.com/litesql/go-ha/blob/main/option.go) file.

## Projects using go-ha

- [HA](https://github.com/litesql/ha): Highly available leaderless SQLite cluster with HTTP and PostgreSQL Wire Protocol
- [PocketBase HA](https://github.com/litesql/pocketbase-ha): Highly available leaderless PocketBase cluster 

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.