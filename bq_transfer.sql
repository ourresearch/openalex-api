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
country text,
country_code text,
continent text,
num_papers numeric)

# heroku run python bq_transfer.py --pg bq_org_name_by_num_papers --bq doiboost.num_dois_by_org_view

create index bq_org_name_by_num_papers_tsvector_idx on bq_org_name_by_num_papers using gin(to_tsvector('english', org_name))
CREATE INDEX bq_org_name_by_num_papers_trgm_idx ON bq_org_name_by_num_papers USING gin (org_name gin_trgm_ops);
CREATE INDEX bq_org_name_by_num_papers_country_idx ON bq_org_name_by_num_papers(country);

# heroku run python bq_transfer.py --pg bq_our_journals_issnl --bq journals.our_journals_issnl_view
drop table bq_our_journals_issnl
create table bq_our_journals_issnl (
-- issnl	text primary key,
issnl	text,
title	text,
sjr	numeric,	
sjr_best_quartile	text,	
h_index	numeric,	
country	text,	
publisher	text,	
categories	text,	
num_articles	numeric,	
num_cc_by	numeric,	
prop_cc_by	numeric,	
prop_oa	numeric,	
num_oa	numeric,	
num_articles_since_2018	numeric,	
num_cc_by_since_2018	numeric,	
prop_cc_by_since_2018	numeric,	
prop_oa_since_2018	numeric,	
num_oa_since_2018	numeric,
five_dois text,
newest_published_date text,
oldest_published_date text,
has_apcs	text,	
apc_url	text,	
apc_fee	numeric,	
apc_currency	text,	
has_submission_fee	BOOLEAN,	
submission_fee_url	text,	
submission_fee	numeric,	
submission_fee_currency	text,	
has_apc_waiver	BOOLEAN,	
apc_waiver_url	text,	
first_year_oa	numeric,	
languages	text,	
editorial_board_url	text,	
review_process	text,	
review_process_url	text,	
aims_scope_url	text,	
instructions_to_authors_url	text,	
plagiarism_screening_policy	BOOLEAN,	
plagiarism_screening_url	text,	
weeks_submission_to_publication	numeric,	
oa_statement_url	text,	
license	text,	
license_attributes	text,	
licence_url	text,	
author_holds_copyright_no_restictions	BOOLEAN,	
copyright_url	text,	
author_holds_publishing_rights_no_restictions	BOOLEAN,	
publishing_rights_url	text
)

CREATE INDEX bq_our_journals_issnl_title_trgm_idx ON bq_our_journals_issnl USING gin (title gin_trgm_ops);

# heroku run python bq_transfer.py --pg bq_scimago_issnl_topics --bq journals.scimago_issnl_topics_view
create table bq_scimago_issnl_topics (
issnl	text,
num_articles_3years	numeric,
topic	text,
quadrant	numeric
)