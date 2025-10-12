package domain

import "time"

const Layout = "02.01.2006"

// business object for the "ownership" CSV.
type Record struct {
	PCV           int
	TypSubjektu   int
	VztahKVozidlu int
	Aktualni      bool
	ICO           *int
	Nazev         *string
	Adresa        *string
	DatumOd       *time.Time
	DatumDo       *time.Time
}
