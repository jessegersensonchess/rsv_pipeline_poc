package domain

import "time"

// Mirrors the CSV and uses pointer types for NULLability like ownership.
type TechInspection struct {
	PCV            int
	Typ            *string // e.g., "E - Evidenční", "P - Pravidelná"
	Stav           *string // e.g., "A", "Nezjištěno"
	KodSTK         *int
	NazevSTK       *string
	PlatnostOd     *time.Time
	PlatnostDo     *time.Time
	CisloProtokolu *string
	Aktualni       bool
}
