CREATE TABLE document (
  source_id         BIGINT PRIMARY KEY,
  filename          TEXT NOT NULL,
  content_type      TEXT NOT NULL,
  object_key        TEXT NOT NULL,
  byte_size         INT  NOT NULL,
  checksum_sha256   TEXT NOT NULL,
  ocr_text          TEXT,
  ocr_confidence    NUMERIC(4,3),
  ocr_page_count    INT,
  ocr_vendor_job_id TEXT,
  migrated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE migration_state (
  source_id            BIGINT PRIMARY KEY,
  lane                 TEXT NOT NULL,
  status               TEXT NOT NULL,
  attempts             INT  NOT NULL DEFAULT 0,
  consecutive_failures INT  NOT NULL DEFAULT 0,
  source_version       BIGINT NOT NULL DEFAULT 0,
  source_created_at    TIMESTAMPTZ,
  checksum_sha256      TEXT,
  ocr_payload          JSONB,
  last_error           TEXT,
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_state_status ON migration_state (status);
CREATE INDEX idx_state_lane_status ON migration_state (lane, status);
CREATE INDEX idx_state_sla ON migration_state (lane, status, source_created_at);

CREATE TABLE migration_event (
  id         BIGSERIAL PRIMARY KEY,
  source_id  BIGINT,
  stage      TEXT NOT NULL,
  lane       TEXT,
  detail     JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_event_source ON migration_event (source_id, id);

CREATE TABLE backfill_checkpoint (
  range_start BIGINT NOT NULL,
  range_end   BIGINT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'PENDING',
  claimed_at  TIMESTAMPTZ,
  PRIMARY KEY (range_start, range_end)
);
CREATE INDEX idx_checkpoint_status ON backfill_checkpoint (status);
