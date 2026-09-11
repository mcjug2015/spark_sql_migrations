-- revision_id:{{revision_id}};
-- prev_revision_id:{{prev_revision_id}};
begin
-- replace the statements below with the idempotent statements you
-- would like to have applied to databricks db or local spark(for testing)
-- crutch scaffolding will use jinja to fill in the cat and schema that are sent to it
-- this is also taken care of in the gh build.

-- e.g:
-- create schema if not exists {{cat}}.{{schema}};
-- create table if not exists {{cat}}.{{schema}}.test_table(int_id bigint, stuff string);
end;