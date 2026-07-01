package producer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"
)

// Store is the DB access layer for the producer (read message shards + advance
// the independent cursor table octo_etl_es_cursor). Abstracted to an interface so
// the chunk pipeline can be unit-tested with a fake store (no real MySQL).
type Store interface {
	// EnsureCursor makes sure the per-shard watermark row exists (first run = 0)
	// so the FOR UPDATE read can always pin a row to serialize on.
	EnsureCursor(ctx context.Context, table string) error
	// DBNowUnix returns DB current time (epoch seconds) — the single time base
	// for the stability gate (avoids app/DB clock skew).
	DBNowUnix(ctx context.Context) (int64, error)
	// ReadStableBatchTx is a short read transaction: FOR UPDATE the cursor row →
	// keyset read one batch → commit immediately. It NEVER does network IO inside
	// the transaction (Kafka produce happens outside, by the caller).
	ReadStableBatchTx(ctx context.Context, table string, batch int) (cursor int64, rows []*srcMessageRow, err error)
	// AdvanceCursor advances the watermark from expected to newID via an optimistic
	// CAS (WHERE last_id=expected). Returns whether a row was actually updated.
	AdvanceCursor(ctx context.Context, table string, expected, newID int64) (bool, error)
	// MaxID returns COALESCE(MAX(id),0) of the shard table — the source watermark
	// for lag (MAX(id) - cursor_position). Observability only.
	MaxID(ctx context.Context, table string) (int64, error)
}

// MySQLStore implements Store over database/sql (keyset pagination, no OFFSET).
//
// The column set matches this repo's backfill source.go and the source module's
// readBatch: body + visibility + Signal flags + created_at (stability gate).
type MySQLStore struct {
	db *sql.DB
}

// NewMySQLStore constructs a MySQLStore.
func NewMySQLStore(db *sql.DB) *MySQLStore {
	return &MySQLStore{db: db}
}

// EnsureCursor inserts the shard watermark row if missing (idempotent).
func (s *MySQLStore) EnsureCursor(ctx context.Context, table string) error {
	if !safeTableName(table) {
		return fmt.Errorf("producer: unsafe table name %q", table)
	}
	_, err := s.db.ExecContext(ctx,
		"INSERT IGNORE INTO octo_etl_es_cursor (shard_table, last_id) VALUES (?, 0)", table)
	return err
}

// DBNowUnix returns UNIX_TIMESTAMP() from the DB.
func (s *MySQLStore) DBNowUnix(ctx context.Context) (int64, error) {
	var now int64
	err := s.db.QueryRowContext(ctx, "SELECT UNIX_TIMESTAMP()").Scan(&now)
	return now, err
}

// ReadStableBatchTx pins the cursor row FOR UPDATE, keyset-reads one batch, then
// commits — holding the row lock only for a local query, never network IO. The
// stable-prefix truncation is done by the caller outside the transaction.
func (s *MySQLStore) ReadStableBatchTx(ctx context.Context, table string, batch int) (cursor int64, rows []*srcMessageRow, err error) {
	if !safeTableName(table) {
		return 0, nil, fmt.Errorf("producer: unsafe table name %q", table)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		// no-op after Commit; on the error paths the rollback releases the
		// FOR UPDATE lock. A rollback error here is not actionable.
		if rerr := tx.Rollback(); rerr != nil && !errors.Is(rerr, sql.ErrTxDone) {
			log.Printf("producer: rollback %s: %v", table, rerr)
		}
	}()

	if err = tx.QueryRowContext(ctx,
		"SELECT last_id FROM octo_etl_es_cursor WHERE shard_table=? FOR UPDATE", table).Scan(&cursor); err != nil {
		return 0, nil, fmt.Errorf("producer: read cursor %s: %w", table, err)
	}

	q := fmt.Sprintf(
		"SELECT id, message_id, message_seq, from_uid, channel_id, channel_type, setting, `signal`, "+
			"`timestamp`, UNIX_TIMESTAMP(created_at) AS created_unix, payload "+
			"FROM `%s` WHERE id>? ORDER BY id ASC LIMIT ?", table)
	res, err := tx.QueryContext(ctx, q, cursor, batch)
	if err != nil {
		return 0, nil, fmt.Errorf("producer: query %s: %w", table, err)
	}
	for res.Next() {
		var r srcMessageRow
		if scanErr := res.Scan(
			&r.ID, &r.MessageID, &r.MessageSeq, &r.FromUID, &r.ChannelID, &r.ChannelType,
			&r.Setting, &r.Signal, &r.Timestamp, &r.CreatedUnix, &r.Payload,
		); scanErr != nil {
			closeRows(res, table)
			return 0, nil, fmt.Errorf("producer: scan %s: %w", table, scanErr)
		}
		rows = append(rows, &r)
	}
	if iterErr := res.Err(); iterErr != nil {
		closeRows(res, table)
		return 0, nil, fmt.Errorf("producer: iterate %s: %w", table, iterErr)
	}
	if closeErr := res.Close(); closeErr != nil {
		return 0, nil, closeErr
	}
	if err = tx.Commit(); err != nil {
		return 0, nil, err
	}
	return cursor, rows, nil
}

// AdvanceCursor advances the watermark with an optimistic CAS.
func (s *MySQLStore) AdvanceCursor(ctx context.Context, table string, expected, newID int64) (bool, error) {
	if !safeTableName(table) {
		return false, fmt.Errorf("producer: unsafe table name %q", table)
	}
	res, err := s.db.ExecContext(ctx,
		"UPDATE octo_etl_es_cursor SET last_id=? WHERE shard_table=? AND last_id=?",
		newID, table, expected)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// MaxID returns COALESCE(MAX(id),0) of the shard table (source watermark for lag).
func (s *MySQLStore) MaxID(ctx context.Context, table string) (int64, error) {
	if !safeTableName(table) {
		return 0, fmt.Errorf("producer: unsafe table name %q", table)
	}
	var maxID int64
	q := fmt.Sprintf("SELECT COALESCE(MAX(id),0) FROM `%s`", table)
	if err := s.db.QueryRowContext(ctx, q).Scan(&maxID); err != nil {
		return 0, fmt.Errorf("producer: max id %s: %w", table, err)
	}
	return maxID, nil
}

// OpenMySQL opens the source DB with an explicit connection pool (no bare driver
// defaults — the plan forbids running on driver defaults).
func OpenMySQL(dsn string, maxOpen, maxIdle int, connMaxLifetime time.Duration) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("producer: open mysql: %w", err)
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(connMaxLifetime)
	return db, nil
}

// closeRows closes a *sql.Rows on an error path, logging any close error (the
// caller is already returning the primary error).
func closeRows(rows *sql.Rows, table string) {
	if cerr := rows.Close(); cerr != nil {
		log.Printf("producer: close rows for %s: %v", table, cerr)
	}
}

// safeTableName allows only [A-Za-z0-9_] (defensive — table names come from
// config). Same discipline as internal/backfill and internal/recon.
func safeTableName(t string) bool {
	if t == "" {
		return false
	}
	for _, r := range t {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// cursorTableDDL mirrors the octo_etl_es_cursor schema (octo-server searchetl
// migration). The producer's cursor table is physically isolated from any other
// ETL cursor — each ETL runs its own watermark.
const cursorTableDDL = "CREATE TABLE IF NOT EXISTS `octo_etl_es_cursor` (" +
	"`shard_table` VARCHAR(64) NOT NULL, " +
	"`last_id` BIGINT NOT NULL DEFAULT 0, " +
	"`updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, " +
	"PRIMARY KEY (`shard_table`)" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"

// EnsureSchema creates the cursor table if it does not exist (idempotent). In
// production the table is provisioned by octo-server's searchetl migration; this
// keeps the standalone binary self-sufficient for isolated stacks / e2e without
// touching the source module.
func (s *MySQLStore) EnsureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, cursorTableDDL)
	if err != nil {
		return fmt.Errorf("producer: ensure cursor table: %w", err)
	}
	return nil
}
