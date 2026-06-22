// Package store provides MySQL device lookup and auto-registration.
// A nil *DeviceStore is valid and approves every device (DB-less mode).
package store

import (
	"context"
	"database/sql"
	"fmt"
	"h02-server/server/internal/config"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

// DeviceStore looks up and auto-creates devices in MySQL.
type DeviceStore struct {
	db  *sql.DB
	cfg *config.Config
	log *zap.Logger
}

// New opens a MySQL connection pool.
func New(cfg *config.Config, log *zap.Logger) (*DeviceStore, error) {
	db, err := sql.Open("mysql", cfg.MySQLDSN())
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	return &DeviceStore{db: db, cfg: cfg, log: log}, nil
}

func (s *DeviceStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// CheckResult is returned by CheckOrCreate.
type CheckResult int

const (
	CheckApproved    CheckResult = iota // device exists and is not blocked → allow
	CheckBlocked                        // device exists but status = blocked → reject
	CheckAutoCreated                    // device was just inserted as pending → allow (approved=false)
)

// CheckOrCreate looks up a device by its broadcast id (the IMEI an H02 device
// emits). A new device is auto-created as `pending` and allowed to connect; a
// `blocked` device is rejected; everything else (pending/active) is allowed.
// If s is nil (DB disabled), every device is treated as approved.
func (s *DeviceStore) CheckOrCreate(ctx context.Context, broadcastID, model string) (CheckResult, error) {
	if s == nil {
		return CheckApproved, nil
	}

	var status string
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT %s FROM %s WHERE %s = ? LIMIT 1`,
			s.cfg.DBStatusColumn,
			s.cfg.DBDevicesTable,
			s.cfg.DBBroadcastColumn,
		), broadcastID,
	).Scan(&status)

	if err == nil {
		if status == "blocked" {
			return CheckBlocked, nil
		}
		return CheckApproved, nil
	}
	if err != sql.ErrNoRows {
		return CheckBlocked, fmt.Errorf("store: lookup broadcast_id %s: %w", broadcastID, err)
	}

	// Not found — auto-create as pending (only the broadcast id is known).
	name := broadcastID
	notes := fmt.Sprintf("Auto-registered via H02. Model: %s", model)
	now := time.Now().UTC().Format("2006-01-02 15:04:05")

	_, err = s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s
			(%s, %s, %s, %s, %s, %s, %s)
		VALUES
			(?, ?, ?, 'pending', ?, ?, ?)
	`,
		s.cfg.DBDevicesTable,
		s.cfg.DBTypeIDColumn,
		s.cfg.DBBroadcastColumn,
		s.cfg.DBNameColumn,
		s.cfg.DBStatusColumn,
		s.cfg.DBNotesColumn,
		s.cfg.DBCreatedAtColumn,
		s.cfg.DBUpdatedAtColumn,
	), s.cfg.DBDeviceTypeID, broadcastID, name, notes, now, now)
	if err != nil {
		return CheckBlocked, fmt.Errorf("store: auto-create broadcast_id %s: %w", broadcastID, err)
	}

	s.log.Info("device auto-created — pending", zap.String("broadcast_id", broadcastID))
	return CheckAutoCreated, nil
}

// RecordHeartbeat stamps the device's last heartbeat and the gap (seconds) since the
// previous beat — its current "heart rate". No-op in DB-less mode (nil receiver).
func (s *DeviceStore) RecordHeartbeat(ctx context.Context, broadcastID string) {
	if s == nil {
		return
	}
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET heartbeat_interval_s = CASE WHEN last_heartbeat_at IS NULL THEN heartbeat_interval_s
		                                ELSE TIMESTAMPDIFF(SECOND, last_heartbeat_at, UTC_TIMESTAMP()) END,
		    last_heartbeat_at = UTC_TIMESTAMP(),
		    last_seen_at = UTC_TIMESTAMP()
		WHERE %s = ?
	`, s.cfg.DBDevicesTable, s.cfg.DBBroadcastColumn), broadcastID)
	if err != nil {
		s.log.Debug("heartbeat update failed", zap.String("broadcast_id", broadcastID), zap.Error(err))
	}
}
