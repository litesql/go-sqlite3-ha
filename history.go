package sqlite3ha

import (
	"github.com/litesql/go-ha"
	"github.com/litesql/go-sqlite3"
)

type historyModule struct {
	connector *ha.Connector
}

func (m *historyModule) EponymousOnlyModule() {}

func (m *historyModule) Create(c *sqlite3.SQLiteConn, args []string) (sqlite3.VTab, error) {
	if err := c.DeclareVTab(ha.HistoryTableDeclaration(args[0])); err != nil {
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
		HistoryCursor: ha.HistoryCursor{Connector: v.connector},
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
	ha.HistoryCursor
}

func (vc *historyCursor) Column(c *sqlite3.SQLiteContext, col int) error {
	switch v := vc.ColumnValue(col).(type) {
	case int64:
		c.ResultInt(int(v))
	case string:
		c.ResultText(v)
	}
	return nil
}

func (vc *historyCursor) Filter(idxNum int, idxStr string, vals []any) error {
	var first any
	if len(vals) > 0 {
		first = vals[0]
	}
	return vc.FilterValue(len(vals) > 0, first)
}
