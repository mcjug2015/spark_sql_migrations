-- revision_id:e5f6a1b2c3d4;
-- prev_revision_id:c3d4e5f6a1b2;
-- a databricks-only statement living in the chain that runs everywhere: jinja
-- drops it before spark sees it, so locally this migration renders to nothing
-- and is recorded without being executed.
{% if is_dbr %}
begin
create volume if not exists {{cat}}.{{schema}}.widget_vol;
end;
{% endif %}
