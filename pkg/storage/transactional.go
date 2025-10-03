package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

func transactional(ctx context.Context, conn *sql.DB, logic func(ctx context.Context, tx *sql.Tx) error) (err error) {
	// Sometimes the mysql driver will return a `mysql.ErrInvalidConn` as this is not retried in stdlib it's done here
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("could not start transaction: %w", err)
	}
	defer func() {
		if rbe := tx.Rollback(); rbe != nil && !errors.Is(rbe, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("could not rollback failed transaction: %w", rbe))
		}
	}()

	if err = logic(ctx, tx); err != nil {
		return fmt.Errorf("transactional logic execution flow errored: %w", err)
	}

	return tx.Commit()
}
