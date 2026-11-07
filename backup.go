package sqlite3ha

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/litesql/go-ha"
	"github.com/litesql/go-sqlite3"
)

func Backup(ctx context.Context, db *sql.DB, w io.Writer) error {
	return ha.BackupWithAdapter(
		ctx,
		db,
		w,
		func(rawConn *sqlite3.SQLiteConn) ([]byte, error) {
			return rawConn.Serialize("")
		},
		func(ctx context.Context, sqliteSrcConn *sqlite3.SQLiteConn, destName string) error {
			destDb, err := sql.Open("sqlite3", destName)
			if err != nil {
				return err
			}
			defer destDb.Close()

			destConn, err := destDb.Conn(ctx)
			if err != nil {
				return err
			}
			defer destConn.Close()

			sqliteDestConn, err := ha.ExtractRawConn[*sqlite3.SQLiteConn](destConn)
			if err != nil {
				return err
			}

			bkp, err := sqliteDestConn.Backup("main", sqliteSrcConn, "main")
			if err != nil {
				return err
			}

			for more := true; more; {
				more, err = bkp.Step(-1)
				if err != nil {
					return fmt.Errorf("backup step error: %w", err)
				}
				if bkp.Remaining() == 0 {
					break
				}
			}

			if err := bkp.Finish(); err != nil {
				return fmt.Errorf("backup finish error: %w", err)
			}

			if err := bkp.Close(); err != nil {
				return fmt.Errorf("backup close error: %w", err)
			}
			return nil
		},
	)
}
