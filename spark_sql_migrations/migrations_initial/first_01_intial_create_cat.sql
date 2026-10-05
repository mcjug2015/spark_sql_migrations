-- revision_id:;
-- a catalog is a unity catalog object: local spark has spark_catalog and no way
-- to make another, so this one is gated to databricks and renders to nothing
-- else, leaving the bootstrap chain the same three files everywhere.
{% if is_dbr %}
begin
create catalog if not exists {{cat}};
end;
{% endif %}
