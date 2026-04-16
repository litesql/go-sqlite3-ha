package sqlite3ha

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/litesql/go-ha"
	haconnect "github.com/litesql/go-ha/connect"
	"github.com/litesql/go-sqlite3"
)

type historyModule struct {
	connector *ha.Connector
}

func (m *historyModule) EponymousOnlyModule() {}

func (m *historyModule) Create(c *sqlite3.SQLiteConn, args []string) (sqlite3.VTab, error) {
	err := c.DeclareVTab(fmt.Sprintf(`
		CREATE TABLE %s (
			seq INT,
			sql TEXT,
			timestamp TEXT
		)`, args[0]))
	if err != nil {
		return nil, err
	}
	return &historyTable{connector: m.connector}, nil
}

func (m *historyModule) Connect(c *sqlite3.SQLiteConn, args []string) (sqlite3.VTab, error) {
	return m.Create(c, args)
}

func (m *historyModule) DestroyModule() {}

type historyTable struct {
	connector *ha.Connector
}

func (v *historyTable) Open() (sqlite3.VTabCursor, error) {
	return &historyCursor{
		connector: v.connector,
	}, nil
}

func (v *historyTable) BestIndex(csts []sqlite3.InfoConstraint, ob []sqlite3.InfoOrderBy) (*sqlite3.IndexResult, error) {
	used := make([]bool, len(csts))
	for i, c := range csts {
		if c.Usable && c.Column == 0 && c.Op == sqlite3.OpGE {
			used[i] = true
			return &sqlite3.IndexResult{
				IdxNum: 1,
				IdxStr: "seq",
				Used:   used,
			}, nil
		}
	}
	return &sqlite3.IndexResult{
		IdxNum: 0,
		IdxStr: "latest",
		Used:   used,
	}, nil
}

func (v *historyTable) Disconnect() error { return nil }
func (v *historyTable) Destroy() error    { return nil }

type historyCursor struct {
	connector *ha.Connector
	pos       int
	rows      []haconnect.HistoryItem
}

func (vc *historyCursor) Column(c *sqlite3.SQLiteContext, col int) error {
	if vc.pos < len(vc.rows) {
		switch col {
		case 0:
			c.ResultInt(int(vc.rows[vc.pos].Seq))
		case 1:
			c.ResultText(strings.Join(vc.rows[vc.pos].SQL, ";\n"))
		case 2:
			c.ResultText(time.Unix(0, vc.rows[vc.pos].Timestamp).Format("2006-01-02T15:04:05.999"))
		}
	}
	return nil
}

func (vc *historyCursor) Filter(idxNum int, idxStr string, vals []any) error {
	var seq uint64
	if len(vals) > 0 {
		switch v := vals[0].(type) {
		case int64:
			if v < 0 {
				v = 1
			}
			seq = uint64(v)
		default:
			return fmt.Errorf("invalid seq value type: %T", vals[0])
		}
	}
	var err error
	vc.rows, err = vc.connector.HistoryBySeq(context.Background(), seq)
	if err != nil {
		return err
	}
	vc.pos = 0
	return nil
}

func (vc *historyCursor) Next() error {
	vc.pos++
	return nil
}

func (vc *historyCursor) EOF() bool {
	return vc.pos >= len(vc.rows)
}

func (vc *historyCursor) Rowid() (int64, error) {
	return int64(vc.pos + 1), nil
}

func (vc *historyCursor) Close() error {
	return nil
}
