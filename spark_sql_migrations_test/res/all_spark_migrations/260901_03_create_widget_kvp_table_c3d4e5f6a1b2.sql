-- revision_id:c3d4e5f6a1b2;
-- prev_revision_id:b2c3d4e5f6a1;
begin
create table if not exists {{cat}}.{{schema}}.widget_kvp(
    widget_id bigint
    , table_key string
    , table_val string
    , created_at TIMESTAMP
);
end;
