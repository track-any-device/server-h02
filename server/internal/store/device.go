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
	CheckApproved    CheckResult = iota // device exists and is approved
	CheckNotApproved                    // device exists but not approved
	CheckAutoCreated                    // device was just inserted (pending approval)
)

// CheckOrCreate looks up a device by IMEI.
// If s is nil (DB disabled), every device is treated as approved.
func (s *DeviceStore) CheckOrCreate(ctx context.Context, imei, model string) (CheckResult, error) {
	if s == nil {
		return CheckApproved, nil
	}

	var isApproved bool
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT %s FROM %s WHERE %s = ? LIMIT 1`,
			s.cfg.DBApprovedColumn,
			s.cfg.DBDevicesTable,
			s.cfg.DBIMEIColumn,
		), imei,
	).Scan(&isApproved)

	if err == nil {
		if isApproved {
			return CheckApproved, nil
		}
		return CheckNotApproved, nil
	}
	if err != sql.ErrNoRows {
		return CheckNotApproved, fmt.Errorf("store: lookup imei %s: %w", imei, err)
	}

	// Not found — auto-create as pending.
	name := fmt.Sprintf("H02 %s", imei)
	notes := fmt.Sprintf("Auto-registered via H02. Model: %s", model)
	now := time.Now().UTC().Format("2006-01-02 15:04:05")

	_, err = s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s
			(%s, %s, %s, %s, %s, %s, %s, %s)
		VALUES
			(?, ?, ?, 'inventory', 0, ?, ?, ?)
	`,
		s.cfg.DBDevicesTable,
		s.cfg.DBTypeIDColumn,
		s.cfg.DBIMEIColumn,
		s.cfg.DBNameColumn,
		s.cfg.DBStatusColumn,
		s.cfg.DBApprovedColumn,
		s.cfg.DBNotesColumn,
		s.cfg.DBCreatedAtColumn,
		s.cfg.DBUpdatedAtColumn,
	), s.cfg.DBDeviceTypeID, imei, name, notes, now, now)
	if err != nil {
		return CheckNotApproved, fmt.Errorf("store: auto-create imei %s: %w", imei, err)
	}

	s.log.Info("device auto-created — pending approval", zap.String("imei", imei))
	return CheckAutoCreated, nil
}
