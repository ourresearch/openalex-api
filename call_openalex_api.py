#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import json
import re
import argparse
import random
from time import time
from itertools import combinations

from app import db
from util import run_sql
from util import safe_commit
from util import Timer
from util import elapsed
from app import get_db_cursor

#
# pick a random number from 1 to 8
# pick that many attributes from list
# for each of them, pick a random setting
# pick an aggregation level (top dois, top journals, top publishers, top countries)

# number of combos of length up to four when have 15 options
# is 15 choose 4 + 15 choose 3 + ... 15 choose 1  https://www.calculatorsoup.com/calculators/discretemathematics/combinations.php?n=15&r=4&action=solve
# = 1365 + 455 + 105 + 15
# = 1940
# 3.5 seconds per = 3.5*1940 = 6790 seconds = 1.9 hours until everything has been primed once

table_lookup = {}
join_lookup = {}

# join_lookup["mag_main_papers"] = ""
# table_lookup["mag_main_papers"] = [
#     ("doi", str),
#     ("doc_type", str),
#     ("year", int),
# ]
#
#
#
# join_lookup["unpaywall"] = " JOIN unpaywall ON unpaywall.doi = mag_main_papers.doi_lower "
# table_lookup["unpaywall"] = [
#     ("doi", str),
#     ("genre", str),
#     # ("journal_is_in_doaj", str),
#     ("journal_is_oa", str),
#     ("oa_status", str),
#     ("best_version", str),
#     ("has_green", bool),
#     ("is_oa_bool", bool),
# ]
#
#
# join_lookup["mag_paperid_affiliations_details"] = " JOIN mag_paperid_affiliations_details ON mag_main_papers.paper_id = mag_paperid_affiliations_details.paper_id "
# table_lookup["mag_paperid_affiliations_details"] = [
#     ("ror_id", str),
#     # ("grid_id", str),
#     ("org", str),
#     ("city", str),
#     # ("region", str),
#     ("state", str),
#     ("country", str),
#     ("continent", str),
# ]
#
# join_lookup["journalsdb_computed"] = """ JOIN mag_main_journals ON mag_main_journals.journal_id = mag_main_papers.journal_id
#                                         JOIN journalsdb_computed_flat ON mag_main_journals.issn = journalsdb_computed_flat.issn
#                                         JOIN journalsdb_computed ON journalsdb_computed_flat.issn_l = journalsdb_computed.issn_l  """
# table_lookup["journalsdb_computed"] = [
#     ("publisher", str),
#     ("issn_l", str),
# ]
# join_lookup["mag_main_authors"] = """ JOIN mag_main_paper_author_affiliations ON mag_main_paper_author_affiliations.paper_id = mag_main_papers.paper_id
#                                         JOIN mag_main_authors ON mag_main_paper_author_affiliations.author_id = mag_main_authors.author_id """
# table_lookup["mag_main_authors"] = [
#     ("normalized_name", str),
#     ("author_id", int),
# ]
#
# join_lookup["unpaywall_oa_location"] = " JOIN unpaywall_oa_location ON unpaywall_oa_location.doi = mag_main_papers.doi_lower "
# table_lookup["unpaywall_oa_location"] = [
#     # ("endpoint_id", str),
#     ("version", str),
#     ("license", str),
#     ("repository_institution", str),
# ]

entity_table_lookup = {
    "works": "mag_combo_all",
    "authors": "mag_paperid_authors",
    "journals": "journalsdb_computed",
    "oa_locations": "unpaywall_paperid_oa_location",
    "fields_of_study": "mag_paperid_fields_of_study"
}

for entity in entity_table_lookup.keys():
    join_lookup[entity] = {}

join_lookup["works"]["mag_combo_all"] = ""
join_lookup["authors"]["mag_combo_all"] = """ JOIN mag_combo_all ON mag_paperid_authors.paper_id = mag_combo_all.paper_id  """
join_lookup["journals"]["mag_combo_all"] = """ JOIN mag_combo_all ON journalsdb_computed.issn_l = mag_combo_all.issn_l  """
join_lookup["oa_locations"]["mag_combo_all"] = """ JOIN mag_combo_all ON unpaywall_paperid_oa_location.paper_id = mag_combo_all.paper_id  """
join_lookup["fields_of_study"]["mag_combo_all"] = """ JOIN mag_combo_all ON mag_paperid_fields_of_study.paper_id = mag_combo_all.paper_id  """
table_lookup["mag_combo_all"] = [
    ("paper_id", int),
    ("doi", str),
    ("doc_type", str),
    ("year", int),
    ("paper_title", str),
    ("journal_title", str),
]


table_lookup["mag_combo_all"] += [
    ("genre", str),
    # ("journal_is_in_doaj", str),
    ("journal_is_oa", str),
    ("oa_status", str),
    ("best_version", str),
    ("has_green", bool),
    ("is_oa_bool", bool),
]


table_lookup["mag_combo_all"] += [
    ("ror_id", str),
    # ("grid_id", str),
    ("org", str),
    ("city", str),
    # ("region", str),
    ("state", str),
    ("country", str),
    ("continent", str),
]

table_lookup["mag_combo_all"] += [
    ("publisher", str),
    ("issn_l", str),
]

join_lookup["works"]["mag_paperid_authors"] = """ JOIN mag_paperid_authors ON mag_paperid_authors.paper_id = mag_combo_all.paper_id """
table_lookup["mag_paperid_authors"] = [
    ("normalized_name", str),
    ("author_id", int),
]

join_lookup["works"]["unpaywall_paperid_oa_location"] = " JOIN unpaywall_paperid_oa_location ON unpaywall_paperid_oa_location.paper_id = mag_combo_all.paper_id "
table_lookup["unpaywall_paperid_oa_location"] = [
    # ("endpoint_id", str),
    ("version", str),
    ("license", str),
    ("repository_institution", str),
]


join_lookup["works"]["mag_paperid_fields_of_study"] = """ JOIN mag_paperid_fields_of_study ON mag_paperid_fields_of_study.paper_id = mag_combo_all.paper_id """
table_lookup["mag_paperid_fields_of_study"] = [
    ("field_of_study_id", int),
    ("normalized_field_of_study_name", str),
]



field_lookup = {}
entities = entity_table_lookup.keys()
for entity in entities:
    field_lookup[entity] = {}
    for table_name in table_lookup:
        for (field, datatype) in table_lookup[table_name]:
            column_dict = {}
            column_dict["table_name"] = table_name
            column_dict["column_name"] = "{}.{}".format(table_name, field)
            column_dict["datatype"] = datatype
            field_lookup[entity][field] = column_dict


max_num_filters = 3
chosen_fields_combinations_remaining = []
all_fields = {}
for entity in entities:
    all_fields[entity] = field_lookup[entity].keys()
# add one for offset
num_groupbys = 1

chosen_fields_combinations_remaining = {}
for entity in entities:
    chosen_fields_combinations_remaining[entity] = []
    for num_filters in range(0, max_num_filters + 1):
        new_combo = list(combinations(all_fields[entity], num_groupbys + num_filters))
        random.shuffle(new_combo)  # randomize within the filter size
        chosen_fields_combinations_remaining[entity] += new_combo

# max_num_filters = len(field_lookup) - 2
# chosen_fields_combinations_remaining = list(combinations(all_fields, num_groupbys + max_num_filters + 1))
# print(chosen_fields_combinations_remaining)

# print chosen_fields_combinations_remaining

# print("Number of fields: {}".format(len(field_lookup.keys())))
# print("Number of tables: {}".format(len(table_lookup.keys())))
# print("Number of combos with {} filters and a group-by: {}".format(max_num_filters, len(chosen_fields_combinations_remaining)))
# print("Number of hours it'd take to go through in a single thread, if 10 seconds each: {}".format(round(len(chosen_fields_combinations_remaining) * 10.0 / 60.0), 1))

def get_column_values(entity, column, random=False, limit=100):
    print("getting values for column {}".format(column))

    global field_lookup
    lookup_dict = field_lookup[entity][column]

    if random:
        orderby = "random()"
    else:
        orderby = "n"

    q = """select {column}, count(*) as n 
        from {table} 
        -- where {column} is not null 
        group by {column} 
        order by {orderby} desc 
        limit {limit}""".format(column=lookup_dict["column_name"], table=lookup_dict["table_name"], orderby=orderby, limit=limit)

    with get_db_cursor() as cursor:
        cursor.execute(q)
        rows = cursor.fetchall()

    return rows


def get_column_values_for_querying(entity, field, random=False):
    column_name_solo = field

    rows = get_column_values(entity, field, random)

    values = []
    for row in rows:
        if isinstance(row[column_name_solo], bool) or isinstance(row[column_name_solo], int) or isinstance(row[column_name_solo], int):
            value = row[column_name_solo]
            values.append(value)
        else:
            # value = row[column_solo].decode('utf-8')
            value = row[column_name_solo]
            if value:
                values.append(value)  # don't include empty strings
    return values

def is_filter_uses_table(entity, filters, table_name):
    for filter in filters:
        (filter_field, filter_value) = filter.split(":", 1)
        if table_name == field_lookup[entity][filter_field]["table_name"]:
            return True
    return False

def is_groupby_uses_table(entity, groupby, table_name):
    if groupby and (table_name == field_lookup[entity][groupby]["table_name"]):
        return True
    return False

def get_work(id_type, id):
    filters = []
    if id_type == "paper_id":
        filters = ["paper_id:{}".format(id)]
    if id_type == "doi":
        filters = ["doi:{}".format(id)]

    (rows, q, timer_dict) = do_query("works", filters, details=True)
    this_work_dict = rows[0]
    keys_to_keep = ["paper_id", "doi", "publication_date", "year", "doc_type", "genre", "issn_l"]
    response_dict = { my_key: this_work_dict[my_key] for my_key in keys_to_keep }

    verbose = True
    (response_dict["authors"], dummy1, dummy2) = do_query("authors", filters, details=True, verbose=verbose)
    (response_dict["oa_locations"], dummy1, dummy2) = do_query("oa_locations", filters, details=True, verbose=verbose)
    (response_dict["fields_of_study"], dummy1, dummy2) = do_query("fields_of_study", filters, details=True, verbose=verbose)
    (response_dict["journal"], dummy1, dummy2) = do_query("journals", filters, details=True, verbose=verbose)

    return (response_dict, timer_dict)


def do_query(entity, filters, searches=[], groupby=None, details=False, limit=100, verbose=True, queryonly=False):
    timer = Timer()

    # just bail on these for now, till we crack these ones
    if is_groupby_uses_table(entity, groupby, "mag_paperid_authors") and is_filter_uses_table(entity, filters, "unpaywall_paperid_oa_location"):
        rows = []
        q = None
        return (rows, q, timer.to_dict())

    join_clause = " "
    for table_name in table_lookup:
        # give priority to groupby, make sure each table is only joined once
        # if is_groupby_uses_table(groupby, table_name):
        #     if join_lookup[entity][table_name] != "" and table_name != "mag_paperid_authors":
        #         join_clause += " LEFT OUTER " + join_lookup[entity][table_name]
        # elif is_filter_uses_table(filters, table_name):
        #     join_clause += join_lookup[entity][table_name]

        # just do simple one for now
        if is_groupby_uses_table(entity, groupby, table_name) or is_filter_uses_table(entity, filters, table_name):
            if join_lookup[entity][table_name] != "":
                join_clause += join_lookup[entity][table_name]
        elif searches != []:
            if is_filter_uses_table(entity, searches, table_name) and join_lookup[entity][table_name]:
                join_clause += join_lookup[entity][table_name]


    filter_string_list = []
    for filter in filters:
        (filter_field, filter_value) = filter.split(":", 1)
        if field_lookup[entity][filter_field]["datatype"] == str:
            filter_value = filter_value.replace("'", "''")
            filter_value = "'{}'".format(filter_value)
        filter_column_name = field_lookup[entity][filter_field]["column_name"]
        filter_string = " ( {} = {} ) ".format(filter_column_name, filter_value)
        filter_string_list.append(filter_string)

    for search in searches:
        (search_field, search_value) = search.split(":", 1)
        search_value = search_value.replace("'", "''")
        search_column_name = field_lookup[entity][search_field]["column_name"]
        search_string = " ( {} ilike '%{}%' ) ".format(search_column_name, search_value)
        filter_string_list.append(search_string)

    if filter_string_list:
        where_clause = " AND ".join(filter_string_list)
    else:
        where_clause = " TRUE "

    groupby_clause = 1
    if groupby:
        groupby_clause = field_lookup[entity][groupby]["column_name"]

    entity_table = entity_table_lookup[entity]

    with get_db_cursor() as cursor:
        timer.log_timing("0. in with")

        if details:
            q = """SELECT distinct paper_id, doi, doc_type, issn_l, paper_title, 
            journal_title, publication_date, year
            from mag_combo_all 
            where paper_id in (
            select distinct mag_combo_all.paper_id 
                    FROM {entity_table}
                    {join_clause} 
                    WHERE {where_clause}
                    ORDER BY mag_combo_all.publication_date DESC
                    LIMIT {limit}
            )
            """.format(
                entity_table=entity_table,
                join_clause=join_clause,
                where_clause=where_clause,
                limit=limit)
        else:
            q = """SELECT {groupby_clause}, count(distinct mag_combo_all.paper_id) as n 
                    FROM mag_combo_all 
                    {join_clause} 
                    WHERE {where_clause}
                    GROUP BY {groupby_clause} 
                    ORDER BY n DESC
                    limit {limit}""".format(
                        entity_table=entity_table,
                        join_clause=join_clause,
                        where_clause=where_clause,
                        groupby_clause=groupby_clause,
                        limit=limit)
        if verbose:
            print("\n\n{}".format(q))

        q = re.sub("\s+", ' ', q)

        if queryonly:
            return (None, q, timer.to_dict())

        cursor.execute(q)
        timer.log_timing("1. after execute")

        rows = cursor.fetchall()
        timer.log_timing("2. after fetchall")

        if details:
            if filters:
                query_string = "https://api.openalex.org/{}/query?filter={}&details".format(entity, ",".join(filters))
            else:
                query_string = "https://api.openalex.org/{}/query?details".format(entity)
        else:
            if filters:
                query_string = "https://api.openalex.org/{}/query?filter={}&groupby={}".format(entity, ",".join(filters), groupby)
            else:
                query_string = "https://api.openalex.org/{}/query?groupby={}".format(entity, groupby)

        print("{:>10}s {:>15,} rows:  {}".format(timer.elapsed_total, len(rows), query_string))

        if not details and (groupby == "mag_combo_all.paper_id"):
            rows = [row["doi"] for row in rows]

        return (rows, q, timer.to_dict())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")
    parser.add_argument('--warm', action='store_true', help="warm cache")
    parser.add_argument('--verbose', action='store_true', help="print verbose")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    entities = ["works"]

    start_time = time()
    print("getting valid column values")
    field_values = {}
    all_fields = {}
    for entity in entities:
        field_values[entity] = {}
        all_fields[entity] = list(field_lookup[entity].keys())
        random.shuffle(all_fields[entity])   # helps be fast in parallel
        for field in all_fields[entity]:
            field_values[entity][field] = get_column_values_for_querying(entity, field)
    print("done, took {} seconds".format(elapsed(start_time)))
    # print(all_fields)

    keep_running = True

    while keep_running:

        for entity in entities:
            if parsed_vars.get("warm"):
                chosen_fields = chosen_fields_combinations_remaining[entity].pop(0)
                # print(chosen_fields)
                if not chosen_fields_combinations_remaining:
                    keep_running = False
            else:
                num_fields = random.randint(num_groupbys, num_groupbys + max_num_filters)
                chosen_fields = random.sample(all_fields[entity], num_fields)

            filters = ["{}:{}".format(c, random.choice(field_values[entity][c])) for c in chosen_fields[num_groupbys:]]
            groupby = chosen_fields[0]

            searches = []
            verbose = parsed_vars.get("verbose")
            (rows, q, timing) = do_query(entity, filters, searches, groupby, verbose=verbose, details=False)
            (rows, q, timing) = do_query(entity, filters, searches, groupby, verbose=verbose, details=True)
