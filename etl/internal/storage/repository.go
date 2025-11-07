package storage

import (
	"context"
	"etl/pkg/records"
)

type Repository interface { BulkUpsert(ctx context.Context, recs []records.Record) error }
