CREATE TABLE IF NOT EXISTS public.vlastnik_vozidla (
    pcv              INTEGER NOT NULL,
    typ_subjektu              TEXT NULL,
    vztah_k_vozidlu             TEXT NULL,
    aktualni     BOOLEAN NULL,
    ico          INTEGER NULL,
    nazev        TEXT NULL,
    adresa       TEXT NULL,
    datum_od     DATE NULL,
    datum_do     DATE NULL,
    id           bigserial PRIMARY KEY
);
