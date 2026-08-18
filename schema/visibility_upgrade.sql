-- Idempotent schema advances applied on top of visibility.sql to reach the
-- version recorded in visibilitySchemaVersion. These run against both a
-- freshly-applied schema and a database initialized by an earlier release,
-- so every statement must be safe to re-run.

-- v1.14: TemporalExternalPayloadSizeBytes and TemporalExternalPayloadCount
-- builtin search attributes.
ALTER TABLE executions_visibility
  ADD COLUMN IF NOT EXISTS TemporalExternalPayloadSizeBytes BIGINT GENERATED ALWAYS AS ((search_attributes->'TemporalExternalPayloadSizeBytes')::bigint) STORED;
ALTER TABLE executions_visibility
  ADD COLUMN IF NOT EXISTS TemporalExternalPayloadCount BIGINT GENERATED ALWAYS AS ((search_attributes->'TemporalExternalPayloadCount')::bigint) STORED;
CREATE INDEX IF NOT EXISTS by_temporal_external_payload_size_bytes ON executions_visibility (namespace_id, TemporalExternalPayloadSizeBytes, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
CREATE INDEX IF NOT EXISTS by_temporal_external_payload_count ON executions_visibility (namespace_id, TemporalExternalPayloadCount, (COALESCE(close_time, '9999-12-31 23:59:59')) DESC, start_time DESC, run_id);
