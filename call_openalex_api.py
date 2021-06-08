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

join_lookup["mag_main_papers"] = ""
table_lookup["mag_main_papers"] = [
    ("doi", str),
    ("doc_type", str),
    ("year", int),
]

join_lookup["mag_main_authors"] = """ JOIN mag_main_paper_author_affiliations ON mag_main_paper_author_affiliations.paper_id = mag_main_papers.paper_id
                                        JOIN mag_main_authors ON mag_main_paper_author_affiliations.author_id = mag_main_authors.author_id """
table_lookup["mag_main_authors"] = [
    ("normalized_name", str),
    ("author_id", int),
]

join_lookup["unpaywall_oa_location"] = " JOIN unpaywall_oa_location ON unpaywall_oa_location.doi = mag_main_papers.doi_lower "
table_lookup["unpaywall_oa_location"] = [
    # ("endpoint_id", str),
    ("version", str),
    ("license", str),
    ("repository_institution", str),
]

join_lookup["unpaywall"] = " JOIN unpaywall ON unpaywall.doi = mag_main_papers.doi_lower "
table_lookup["unpaywall"] = [
    ("genre", str),
    # ("journal_is_in_doaj", str),
    ("journal_is_oa", str),
    ("oa_status", str),
    ("best_version", str),
    ("has_green", bool),
    ("is_oa_bool", bool),
]


join_lookup["mag_paperid_affiliations_details"] = " JOIN mag_paperid_affiliations_details ON mag_main_papers.paper_id = mag_paperid_affiliations_details.paper_id "
table_lookup["mag_paperid_affiliations_details"] = [
    ("ror_id", str),
    # ("grid_id", str),
    ("org", str),
    ("city", str),
    # ("region", str),
    ("state", str),
    ("country", str),
    ("continent", str),
]

join_lookup["journalsdb_computed"] = """ JOIN mag_main_journals ON mag_main_journals.journal_id = mag_main_papers.journal_id
                                        JOIN journalsdb_computed_flat ON mag_main_journals.issn = journalsdb_computed_flat.issn
                                        JOIN journalsdb_computed ON journalsdb_computed_flat.issn_l = journalsdb_computed.issn_l  """
table_lookup["journalsdb_computed"] = [
    ("publisher", str),
    ("issn_l", str),
]

join_lookup["mag_paperid_fields_of_study"] = """ JOIN mag_paperid_fields_of_study ON mag_paperid_fields_of_study.paper_id = mag_main_papers.paper_id """
table_lookup["mag_paperid_fields_of_study"] = [
    ("field_of_study_id", int),
    ("normalized_field_of_study_name", str),
]




field_lookup = {}
for table_name in table_lookup:
    for (field, datatype) in table_lookup[table_name]:
        column_dict = {}
        column_dict["table_name"] = table_name
        column_dict["column_name"] = "{}.{}".format(table_name, field)
        column_dict["datatype"] = datatype
        field_lookup[field] = column_dict


max_num_filters = 3
chosen_fields_combinations_remaining = []
all_fields = field_lookup.keys()
# add one for offset
num_groupbys = 1
for num_filters in range(0, max_num_filters + 1):
    chosen_fields_combinations_remaining += combinations(all_fields, num_groupbys + num_filters)
# print chosen_fields_combinations_remaining
random.shuffle(chosen_fields_combinations_remaining)


print("Number of fields: {}".format(len(field_lookup.keys())))
print("Number of tables: {}".format(len(table_lookup.keys())))
print("Number of combos with {} filters and a group-by: {}".format(max_num_filters, len(chosen_fields_combinations_remaining)))
print("Number of hours it'd take to go through in a single thread, if 10 seconds each: {}".format(round(len(chosen_fields_combinations_remaining) * 10.0 / 60.0), 1))

def get_column_values(column, random=False, limit=100):
    print("getting values for column {}".format(column))

    global field_lookup
    lookup_dict = field_lookup[column]

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


def get_column_values_for_querying(field, random=False):
    column_name_solo = field

    rows = get_column_values(field, random)

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

def is_filter_uses_table(filters, table_name):
    for filter in filters:
        (filter_field, filter_value) = filter.split(":", 1)
        if table_name == field_lookup[filter_field]["table_name"]:
            return True
    return False

def is_groupby_uses_table(groupby, table_name):
    if groupby and (table_name == field_lookup[groupby]["table_name"]):
        return True
    return False

def do_query(filters, groupby=None, details=False, limit=100, verbose=True, queryonly=False):
    timer = Timer()

    join_clause = " "
    for table_name in table_lookup:
        # give priority to groupby, make sure each table is only joined once
        if is_groupby_uses_table(groupby, table_name):
            if join_lookup[table_name] != "":
                join_clause += " LEFT OUTER " + join_lookup[table_name]
        elif is_filter_uses_table(filters, table_name):
            join_clause += join_lookup[table_name]

    filter_string_list = []
    for filter in filters:
        (filter_field, filter_value) = filter.split(":", 1)
        if field_lookup[filter_field]["datatype"] == str:
            filter_value = filter_value.replace("'", "''")
            filter_value = "'{}'".format(filter_value)
        filter_column_name = field_lookup[filter_field]["column_name"]
        filter_string = " ( {} = {} ) ".format(filter_column_name, filter_value)
        filter_string_list.append(filter_string)

    if filter_string_list:
        where_clause = " AND ".join(filter_string_list)
    else:
        where_clause = " TRUE "

    groupby_clause = 1
    if groupby:
        groupby_clause = field_lookup[groupby]["column_name"]

    with get_db_cursor() as cursor:
        timer.log_timing("0. in with")

        if details:
            q = """SELECT mag_main_papers.*
                    FROM mag_main_papers
                    {join_clause} 
                    WHERE {where_clause}
                    ORDER BY mag_main_papers.publication_date DESC
                    LIMIT {limit}
                    """.format(
                        join_clause=join_clause,
                        where_clause=where_clause,
                        limit=limit)
        else:
            q = """SELECT {groupby_clause}, count(distinct mag_main_papers.paper_id) as n 
                    FROM mag_main_papers 
                    {join_clause} 
                    WHERE {where_clause}
                    GROUP BY {groupby_clause} 
                    ORDER BY n DESC
                    limit {limit}
                    """.format(
                        join_clause=join_clause,
                        where_clause=where_clause,
                        groupby_clause=groupby_clause,
                        limit=limit)
        if verbose:
            print(q)

        q = re.sub("\s+", ' ', q)

        if queryonly:
            return (None, q, timer.to_dict())

        cursor.execute(q)
        timer.log_timing("1. after execute")

        rows = cursor.fetchall()
        timer.log_timing("2. after fetchall")

        if filters:
            query_string = "https://api.openalex.org/works/query?filter={}&groupby={}".format(",".join(filters), groupby)
        else:
            query_string = "https://api.openalex.org/works/query?groupby={}".format(groupby)

        print("{:>10}s {:>15,} rows:  {}".format(timer.elapsed_total, len(rows), query_string))

        if not details and (groupby == "mag_main_papers.paper_id"):
            rows = [row["doi"] for row in rows]

        return (rows, q, timer.to_dict())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")
    parser.add_argument('--warm', action='store_true', help="warm cache")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    start_time = time()
    print("getting valid column values")
    all_fields = list(field_lookup.keys())
    random.shuffle(all_fields)   # helps be fast in parallel
    field_values = {}
    for field in all_fields:
        field_values[field] = get_column_values_for_querying(field)
    print("done, took {} seconds".format(elapsed(start_time)))

    keep_running = True

    while keep_running:
        if parsed_vars.get("warm"):
            chosen_fields = chosen_fields_combinations_remaining.pop()
            if not chosen_fields_combinations_remaining:
                keep_running = False
        else:
            num_fields = random.randint(num_groupbys, num_groupbys + max_num_filters)
            chosen_fields = random.sample(all_fields, num_fields)

        filters = ["{}:{}".format(c, random.choice(field_values[c])) for c in chosen_fields[num_groupbys:]]
        groupby = chosen_fields[0]

        (rows, q, timing) = do_query(filters, groupby, verbose=False)
