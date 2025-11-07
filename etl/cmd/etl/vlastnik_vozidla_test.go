package main

import (
	"context"
	"encoding/json"
	"etl/internal/config"
	"os"
	"testing"
	"time"
)

func TestVlastnikVozidlaPipeline(t *testing.T) {
	// Path to the config file for the VlastnikVozidla pipeline
	cfgPath := "../../configs/pipelines/vlastnik_vozidla.json"

	// Open the config file
	f, err := os.Open(cfgPath)
	if err != nil {
		t.Fatalf("open config: %v", err)
	}
	defer f.Close()

	var p config.Pipeline
	if err := json.NewDecoder(f).Decode(&p); err != nil {
		t.Fatalf("decode config: %v", err)
	}

	// Create context
	ctx := context.Background()

	// Pass the current time as the start time (not nil)
	start := time.Now()

	// Run the pipeline
	if err := run(ctx, p, true, start); err != nil {
		t.Fatalf("run pipeline: %v", err)
	}
}
