create table bq_journals (
issn text,
journal_name text,
num numeric);

create index bq_journals_issn_idx on bq_journals(issn);

