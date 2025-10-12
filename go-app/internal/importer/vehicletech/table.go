package vehicletech

import (
	"context"
	"fmt"
	"strings"

	"csvloader/internal/db"
)

// todo: refactor with small db interface to remove inline db specifics
func ensureVehicleTechTable(ctx context.Context, d db.DB, unlogged bool, driver string) error {
	switch strings.ToLower(driver) {
	case "postgres":
		// Use JSONB for indexing speed; JSON also fine if you prefer.
		if err := d.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS vehicle_tech (
				pcv BIGINT,
				payload JSONB
			);
		`); err != nil {
			return fmt.Errorf("create vehicle_tech (pg): %w", err)
		}
		if unlogged {
			_ = d.Exec(ctx, `ALTER TABLE vehicle_tech SET UNLOGGED`)
		}
		// Helpful index
		_ = d.Exec(ctx, `CREATE INDEX IF NOT EXISTS vehicle_tech_pcv_idx ON vehicle_tech(pcv)`)
		return nil

	case "mssql":
		// T-SQL: create-if-missing + enforce valid JSON
		if err := d.Exec(ctx, `
			IF OBJECT_ID(N'vehicle_tech', N'U') IS NULL
			BEGIN
				CREATE TABLE vehicle_tech (
					pcv BIGINT,
					payload NVARCHAR(MAX) CHECK (ISJSON(payload) = 1)
				);
			END
		`); err != nil {
			return fmt.Errorf("create vehicle_tech (mssql): %w", err)
		}
		// Helpful index
		_ = d.Exec(ctx, `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'vehicle_tech_pcv_idx')
			CREATE INDEX vehicle_tech_pcv_idx ON vehicle_tech(pcv)`)
		return nil

	default:
		return fmt.Errorf("unknown driver: %s", driver)
	}
}
