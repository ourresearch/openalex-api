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

