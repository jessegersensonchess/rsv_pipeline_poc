CREATE TABLE IF NOT EXISTS public.technicke_prohlidky (
    pcv              INTEGER NOT NULL,
    typ              TEXT NULL,
    stav             TEXT NULL,
    kod_stk         INTEGER NULL,
    nazev_stk       TEXT NULL,
    platnost_od     DATE NULL,
    platnost_do     DATE NULL,
    cislo_protokolu TEXT NULL,
    aktualni        BOOLEAN NULL,
	id 				bigserial PRIMARY KEY
);

