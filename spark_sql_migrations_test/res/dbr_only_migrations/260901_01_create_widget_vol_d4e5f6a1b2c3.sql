-- revision_id:d4e5f6a1b2c3;
-- prev_revision_id:;
begin
create volume if not exists {{cat}}.{{schema}}.widget_vol;
end;
