-- revision_id:;
begin
create table if not exists {{cat}}.{{schema}}._spark_migrations_version (
    migration_type string not null,
    version_num string not null
);
end;