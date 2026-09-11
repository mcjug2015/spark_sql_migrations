-- revision_id:b2c3d4e5f6a1;
-- prev_revision_id:a1b2c3d4e5f6;
begin
  -- idempotent add: swallow FIELD_ALREADY_EXISTS (SQLSTATE 42710) if the column
  -- is already present. Databricks has no ADD COLUMN IF NOT EXISTS, so use a
  -- SQL-scripting EXIT handler (CONTINUE handlers are unsupported).
  declare exit handler for sqlstate '42710'
  begin end;
  alter table {{cat}}.{{schema}}.widget add column widget_batch_id string;
end;
