-- Approved manual planning history. Run explicitly; never recreated by ETL.
CREATE TABLE IF NOT EXISTS public.mrp_review_history (
    id UUID PRIMARY KEY,
    qb_num TEXT NOT NULL CHECK (btrim(qb_num) <> ''),
    assessed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_id TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    reviewed_by TEXT NOT NULL CHECK (btrim(reviewed_by) <> ''),
    no_blockers BOOLEAN NOT NULL,
    blockers JSONB NOT NULL CHECK (jsonb_typeof(blockers) = 'array'),
    system_suggestions JSONB NOT NULL,
    notes TEXT NOT NULL DEFAULT '',
    incident_id UUID,
    source_revision INTEGER,
    review_kind TEXT NOT NULL DEFAULT 'initial',
    mes_sent_at TIMESTAMPTZ,
    CHECK ((no_blockers AND jsonb_array_length(blockers) = 0)
        OR (NOT no_blockers AND jsonb_array_length(blockers) > 0)),
    UNIQUE (id, qb_num)
);
CREATE INDEX IF NOT EXISTS mrp_review_history_so_time ON public.mrp_review_history (qb_num, assessed_at DESC);
CREATE TABLE IF NOT EXISTS public.lt_planning_history (
    id UUID PRIMARY KEY,
    qb_num TEXT NOT NULL,
    review_id UUID NOT NULL,
    planned_production_start DATE NOT NULL,
    assigned_ship_date DATE CHECK (assigned_ship_date < DATE '2099-12-31'),
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    mes_sent_at TIMESTAMPTZ,
    assigned_by TEXT NOT NULL CHECK (btrim(assigned_by) <> ''),
    blocker_override BOOLEAN NOT NULL DEFAULT FALSE,
    notes TEXT NOT NULL DEFAULT '',
    CHECK (planned_production_start <= assigned_ship_date),
    FOREIGN KEY (review_id, qb_num) REFERENCES public.mrp_review_history (id, qb_num)
);
-- Existing deployments: plans can be recorded before CLI imports an assigned LT.
ALTER TABLE public.lt_planning_history ALTER COLUMN assigned_ship_date DROP NOT NULL;
ALTER TABLE public.lt_planning_history ADD COLUMN IF NOT EXISTS mes_sent_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS lt_planning_history_so_time ON public.lt_planning_history (qb_num, assigned_at DESC);
COMMENT ON TABLE public.mrp_review_history IS 'Append-only human-approved material assessments; empty blockers explicitly means no blockers. Quantities are independent per-SO, not additive purchasing requirements.';
COMMENT ON TABLE public.lt_planning_history IS 'Append-only production planning decisions. Ship date is a nullable snapshot of CLI-imported SO data at save time, never manually entered. assigned_at is the plan save timestamp, not the source LT assignment time. Confirmed saves atomically update MES shared production schedule and mes_sent_at. Does not update source SO dates or MES execution.';
