CREATE TABLE IF NOT EXISTS public.hr_events (
  pcv       integer     NOT NULL,
  date_from date        NULL,
  date_to   date        NULL,
  reason    text        NULL,
  rm_code   integer     NULL,
  rm_name   text        NULL,
PRIMARY KEY (pcv, date_from)
);

#CREATE UNIQUE INDEX IF NOT EXISTS hr_events_pcv_date_from_uidx
#ON public.hr_events (pcv, date_from);

ALTER TABLE public.hr_events
  ADD COLUMN IF NOT EXISTS id bigserial PRIMARY KEY;
