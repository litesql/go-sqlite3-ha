package sqlite3ha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/litesql/go-ha"
	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	"github.com/litesql/go-sqlite3"
)

type connHooksProvider struct {
	nodeName             string
	replicationID        string
	disableDDLSync       bool
	publisher            ha.Publisher
	cdcPublisher         ha.CDCPublisher
	leader               ha.LeaderProvider
	txseqTrackerProvider ha.TxSeqTrackerProvider
	grpcTimeout          time.Duration
	grpcToken            string
	grpcInsecure         bool
	queryRouter          *regexp.Regexp
}

func newConnHooksProvider(cfg ha.ConnHooksConfig) *connHooksProvider {
	return &connHooksProvider{
		nodeName:             cfg.NodeName,
		replicationID:        cfg.ReplicationID,
		disableDDLSync:       cfg.DisableDDLSync,
		publisher:            cfg.Publisher,
		cdcPublisher:         cfg.CDC,
		txseqTrackerProvider: cfg.TxSeqTrackerProvider,
		leader:               cfg.Leader,
		grpcTimeout:          cfg.GrpcTimeout,
		grpcToken:            cfg.GrpcToken,
		grpcInsecure:         cfg.GrpcInsecure,
		queryRouter:          cfg.QueryRouter,
	}
}

func (p *connHooksProvider) RegisterHooks(c driver.Conn, connector *ha.Connector) (driver.Conn, error) {
	sqliteConn, _ := c.(*sqlite3.SQLiteConn)
	err := sqliteConn.RegisterFunc("ha_undo", func(seq int64) string {
		if seq < 0 {
			return "param must be non-negative"
		}
		err := connector.UndoBySeq(context.Background(), uint64(seq))
		if err != nil {
			return err.Error()
		}
		if seq == 0 {
			return "undone last transaction"
		}
		return fmt.Sprintf("undone transactions until stream sequence %d", seq)
	}, false)
	if err != nil {
		return nil, fmt.Errorf("failed to register ha_undo function: %w", err)
	}
	err = sqliteConn.CreateModule("ha_history", &historyModule{
		connector: connector,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create ha_history vtab module: %w", err)
	}
	enableCDCHooks(sqliteConn, p.nodeName, p.replicationID, p.publisher, p.cdcPublisher)
	conn := &Conn{
		SQLiteConn:     sqliteConn,
		disableDDLSync: p.disableDDLSync,
		enableRedirect: true,
		replicationID:  p.replicationID,
		leader:         p.leader,
		reqCh:          make(chan *sqlv1.QueryRequest),
		resCh:          make(chan *sqlv1.QueryResponse),
		txseqTracker:   p.txseqTrackerProvider(),
		timeout:        p.grpcTimeout,
		token:          p.grpcToken,
		insecure:       p.grpcInsecure,
		queryRouter:    p.queryRouter,
	}

	return conn, conn.start()
}

func (p *connHooksProvider) DisableHooks(conn *sql.Conn) error {
	sconn, err := haSqliteConn(conn)
	if err != nil {
		return err
	}
	sconn.RegisterPreUpdateHook(nil)
	sconn.RegisterCommitHook(nil)
	sconn.RegisterRollbackHook(nil)
	sconn.enableRedirect = false
	return nil
}

func (p *connHooksProvider) EnableHooks(conn *sql.Conn) error {
	sconn, err := haSqliteConn(conn)
	if err != nil {
		return err
	}
	enableCDCHooks(sconn.SQLiteConn, p.nodeName, p.replicationID, p.publisher, p.cdcPublisher)
	sconn.enableRedirect = true
	return sconn.start()
}

type tableSchema struct {
	columns, types, pkColumns []string
}

var tableSchemaCache = make(map[string]map[string]*tableSchema)

func clearTableSchemaCache(replicationID string) {
	delete(tableSchemaCache, replicationID)
}

func getTableSchema(sconn *sqlite3.SQLiteConn, replicationID string, database string, table string) (*tableSchema, error) {
	key := fmt.Sprintf("%s.%s", database, table)
	replicationTables, ok := tableSchemaCache[replicationID]
	if !ok {
		replicationTables = map[string]*tableSchema{}
		tableSchemaCache[replicationID] = replicationTables
	} else {
		if schema, ok := replicationTables[key]; ok {
			return schema, nil
		}
	}
	var schema tableSchema
	rows, err := sconn.Query(fmt.Sprintf("SELECT name, type, pk FROM %s.PRAGMA_TABLE_INFO('%s') ORDER BY cid", database, table), nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for {
		dataRow := []driver.Value{new(string), new(string), new(int64)}

		err := rows.Next(dataRow)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Error("failed to read table columns", "error", err, "table", table)
			}
			break
		}
		if v, ok := dataRow[0].(string); ok {
			schema.columns = append(schema.columns, v)
		} else {
			continue
		}
		if v, ok := dataRow[1].(string); ok {
			schema.types = append(schema.types, v)
		}
		if v, ok := dataRow[2].(int64); ok && v > 0 {
			schema.pkColumns = append(schema.pkColumns, dataRow[0].(string))
		}
	}
	replicationTables[key] = &schema
	return &schema, nil

}

func enableCDCHooks(sconn *sqlite3.SQLiteConn, nodeName, replicationID string, publisher ha.Publisher, cdc ha.CDCPublisher) {
	changeSetSessionsMu.Lock()
	defer changeSetSessionsMu.Unlock()

	cs := ha.NewChangeSet(nodeName, replicationID)
	changeSetSessions[sconn] = cs
	sconn.RegisterPreUpdateHook(func(d sqlite3.SQLitePreUpdateData) {
		change, ok := getChange(&d)
		if !ok {
			return
		}
		schema, err := getTableSchema(sconn, replicationID, change.Database, change.Table)
		if err != nil {
			slog.Error("failed to read columns", "error", err, "database", change.Database, "table", change.Table)
			return
		}
		change.Columns = schema.columns
		change.PKColumns = schema.pkColumns
		for i, t := range schema.types {
			if t != "BLOB" {
				if i < len(change.OldValues) && change.OldValues[i] != nil {
					change.OldValues[i] = convert(change.OldValues[i])
				}
				if i < len(change.NewValues) && change.NewValues[i] != nil {
					change.NewValues[i] = convert(change.NewValues[i])
				}
			}
		}

		cs.AddChange(change)
	})

	sconn.RegisterCommitHook(func() int {
		if err := cs.Send(publisher); err != nil {
			slog.Error("failed to send changeset", "error", err)
			return 1
		}
		if cdc != nil {
			data := cs.DebeziumData()
			if len(data) > 0 {
				if err := cdc.Publish(data); err != nil {
					slog.Error("failed to send cdc", "error", err)
					return 1
				}
			}
		}
		return 0
	})
	sconn.RegisterRollbackHook(func() {
		cs.Clear()
	})
}

var (
	changeSetSessions   = make(map[*sqlite3.SQLiteConn]*ha.ChangeSet)
	changeSetSessionsMu sync.RWMutex
)

func addSQLChange(conn *sqlite3.SQLiteConn, sql string, args []any) error {
	changeSetSessionsMu.RLock()
	defer changeSetSessionsMu.RUnlock()

	cs := changeSetSessions[conn]
	if cs == nil {
		return errors.New("no changeset session for the connection")
	}
	cs.AddChange(ha.Change{
		Operation: "SQL",
		Command:   sql,
		Args:      args,
	})
	return nil
}

func removeLastChange(conn *sqlite3.SQLiteConn) error {
	changeSetSessionsMu.RLock()
	defer changeSetSessionsMu.RUnlock()

	cs := changeSetSessions[conn]
	if cs == nil {
		return errors.New("no changeset session for the connection")
	}
	if len(cs.Changes) > 0 {
		cs.Changes = cs.Changes[:len(cs.Changes)-1]
	}
	return nil
}

func convert(src any) any {
	switch v := src.(type) {
	case []byte:
		return string(v)
	default:
		return src
	}
}

func getChange(d *sqlite3.SQLitePreUpdateData) (c ha.Change, ok bool) {
	ok = true
	c = ha.Change{
		Database: d.DatabaseName,
		Table:    d.TableName,
		OldRowID: d.OldRowID,
		NewRowID: d.NewRowID,
	}
	count := d.Count()
	switch d.Op {
	case sqlite3.SQLITE_UPDATE:
		c.Operation = "UPDATE"
		c.OldValues = make([]any, count)
		c.NewValues = make([]any, count)
		for i := range count {
			c.OldValues[i] = &c.OldValues[i]
			c.NewValues[i] = &c.NewValues[i]
		}
		d.Old(c.OldValues...)
		d.New(c.NewValues...)
	case sqlite3.SQLITE_INSERT:
		c.Operation = "INSERT"
		c.NewValues = make([]any, count)
		for i := range count {
			c.NewValues[i] = &c.NewValues[i]
		}
		d.New(c.NewValues...)
	case sqlite3.SQLITE_DELETE:
		c.Operation = "DELETE"
		c.OldValues = make([]any, count)
		for i := range count {
			c.OldValues[i] = &c.OldValues[i]
		}
		d.Old(c.OldValues...)
	default:
		c.Operation = fmt.Sprintf("UNKNOWN - %d", d.Op)
	}

	return
}
