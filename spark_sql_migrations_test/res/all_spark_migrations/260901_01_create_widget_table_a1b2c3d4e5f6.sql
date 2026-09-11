-- revision_id:a1b2c3d4e5f6;
-- prev_revision_id:;
begin
create table if not exists {{cat}}.{{schema}}.widget(widget_id bigint, label string);
end;
