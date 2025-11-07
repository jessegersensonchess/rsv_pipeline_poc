package schema

import "time"

const Layout = "02.01.2006"

type VozidlaVyrazena struct {
	PCV      int        `db:"pcv"`
	DateFrom *time.Time `db:"date_from"`
	DateTo   *time.Time `db:"date_to"`
	Reason   *string    `db:"reason"`
	RMCode   *int       `db:"rm_code"`
	RMName   *string    `db:"rm_name"`
}

type TechnickeProhlidky struct {
	PCV            int        `db:"pcv"`
	Typ            *string    `db:"typ"`  // e.g., "E - Evidenční", "P - Pravidelná"
	Stav           *string    `db:"stav"` // e.g., "A", "Nezjištěno"
	KodSTK         *int       `db:"kod_stk"`
	NazevSTK       *string    `db:"nazev_stk"`
	PlatnostOd     *time.Time `db:"platnost_od"`
	PlatnostDo     *time.Time `db:"platnost_do"`
	CisloProtokolu *string    `db:"cislo_protokolu"`
	Aktualni       bool       `db:"aktualni"`
}

// VlastnikVozidla represents an ownership record for vehicles.
type VlastnikVozidla struct {
	PCV           int        `db:"pcv"`
	TypSubjektu   *string    `db:"typ_subjektu"`    // e.g., "E - Evidenční", "P - Pravidelná"
	VztahKVozidlu *string    `db:"vztah_k_vozidlu"` // e.g., "A", "Nezjištěno"
	Aktualni      bool       `db:"aktualni"`
	ICO           *int       `db:"ico"`
	Adresa        *string    `db:"adresa"`
	Nazev         *string    `db:"nazev"`
	DatumOd       *time.Time `db:"datum_od"`
	DatumDo       *time.Time `db:"datum_do"`
}

type ZpravyVyrobceZastupce struct {
	PCV        int  `db:"pcv"`
	KratkyText bool `db:"kratky_text"`
}
