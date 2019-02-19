create table bq_journals (
issn text,
journal_name text,
num numeric);

create index bq_journals_issn_idx on bq_journals(issn);

create table bq_grid_base (
id text,
org text,
city text,
region text,
country text);

create table bq_org_name_by_num_papers (
org_name text,
grid_id text,
num_papers numeric)

create index bq_org_name_by_num_papers_tsvector_idx on bq_org_name_by_num_papers using gin(to_tsvector('english', org_name))
CREATE INDEX bq_org_name_by_num_papers_trgm_idx ON bq_org_name_by_num_papers USING gin (org_name gin_trgm_ops);