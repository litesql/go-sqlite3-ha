package sqlite3ha

import (
	"database/sql/driver"
	"fmt"

	"github.com/litesql/go-ha"
	haconnect "github.com/litesql/go-ha/connect"
	"github.com/litesql/go-sqlite3"
)

type hooksAdapter struct{}

func (a *hooksAdapter) WrapConn(c driver.Conn, connector *ha.Connector) (ha.SQLiteConn, error) {
	sqliteConn, ok := c.(*sqlite3.SQLiteConn)
	if !ok {
		return nil, fmt.Errorf("not a sqlite3 connection: %T", c)
	}
	if err := sqliteConn.RegisterFunc("ha_undo", ha.UndoFunc(connector, haconnect.UndoFilterNone), false); err != nil {
		return nil, fmt.Errorf("failed to register ha_undo function: %w", err)
	}
	if err := sqliteConn.RegisterFunc("ha_undoe", ha.UndoFunc(connector, haconnect.UndoFilterEntity), false); err != nil {
		return nil, fmt.Errorf("failed to register ha_undoe function: %w", err)
	}
	if err := sqliteConn.RegisterFunc("ha_undot", ha.UndoFunc(connector, haconnect.UndoFilterTransaction), false); err != nil {
		return nil, fmt.Errorf("failed to register ha_undot function: %w", err)
	}
	if err := sqliteConn.CreateModule("ha_history", &historyModule{connector: connector}); err != nil {
		return nil, fmt.Errorf("failed to create ha_history vtab module: %w", err)
	}
	return sqliteConn, nil
}

func (a *hooksAdapter) EnableHooks(conn ha.SQLiteConn, cb ha.HookCallbacks) {
	sconn := conn.(*sqlite3.SQLiteConn)
	sconn.RegisterPreUpdateHook(func(d sqlite3.SQLitePreUpdateData) {
		if d.TableName == ha.TwoPhaseCommitDecisionTable || d.TableName == ha.OutboxTable {
			return
		}
		if change, ok := getChange(&d); ok {
			cb.OnPreUpdate(change)
		}
	})
	sconn.RegisterCommitHook(func() int {
		if err := cb.OnCommit(); err != nil {
			return 1
		}
		return 0
	})
	sconn.RegisterRollbackHook(cb.OnRollback)
}

func (a *hooksAdapter) DisableHooks(conn ha.SQLiteConn) {
	sconn := conn.(*sqlite3.SQLiteConn)
	sconn.RegisterPreUpdateHook(nil)
	sconn.RegisterCommitHook(nil)
	sconn.RegisterRollbackHook(nil)
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
