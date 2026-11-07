package sqlite3ha_test

import (
	"testing"

	"github.com/litesql/go-ha/hatest"
	sqlite3ha "github.com/litesql/go-sqlite3-ha"
)

func TestDriverSuite(t *testing.T) {
	hatest.RunDriverSuite(t, sqlite3ha.NewConnector, nil)
}

func TestResilienceSuite(t *testing.T) {
	hatest.RunResilienceSuite(t, "sqlite3-ha")
}
