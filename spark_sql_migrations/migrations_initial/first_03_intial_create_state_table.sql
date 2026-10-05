-- revision_id:;
begin
create table if not exists {{cat}}.{{schema}}._spark_migrations_version (
    version_num string not null
);
end;
