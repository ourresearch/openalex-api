#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import json
import re
import argparse
import random
from time import time

from app import db
from util import run_sql
from util import safe_commit
from util import elapsed
from app import get_db_cursor

#
# pick a random number from 1 to 8
# pick that many attributes from list
# for each of them, pick a random setting
# pick an aggregation level (top dois, top journals, top publishers, top countries)

all_columns = ['v.doi', 'v.is_oa', 'v.has_hybrid', 'v.has_green', 'v.has_gold', 'v.has_bronze',
               'v.is_in_doaj', 'v.issnl', 'v.publisher', 'v.year', 'v.city', 'v.state', 'v.country',
               'v.continent', 'v.subcontinent', 'a.normalized_name']

# number of combos of length up to four
# is 15 choose 4 + 15 choose 3 + ... 15 choose 1  https://www.calculatorsoup.com/calculators/discretemathematics/combinations.php?n=15&r=4&action=solve
# = 1365 + 455 + 105 + 15
# = 1940
# 3.5 seconds per = 3.5*1940 = 6790 seconds = 1.9 hours until everything has been primed once

def get_column_values(column):
    print u"getting values for column {}".format(column)
    (column_table, column_solo) = column.split(".")
    join_with_a = (column_table == "a")
    join_clause = u" "
    if join_with_a:
        join_clause += u" join mag_authors_paperid a on v.doi=a.doi "

    with get_db_cursor() as cursor:
        q = "select {} from ricks_temp_pub_affil_journals2 v {} where {} is not null order by random() limit 100".format(column, join_clause, column)
        cursor.execute(q)
        rows = cursor.fetchall()
    values = []
    for row in rows:
        if isinstance(row[column_solo], bool) or isinstance(row[column_solo], int) or isinstance(row[column_solo], long):
            values.append(row[column_solo])
        else:
            values.append(u"'{}'".format(row[column_solo].decode('utf-8')))
    return values


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")
    parser.add_argument('--pg', nargs="?", type=str, help="table name in postgres (eg bq_journals)")
    parser.add_argument('--bq', nargs="?", type=str, help="table name in bigquery (eg unpaywall.journals)")

    parsed_args = parser.parse_args()

    start_time = time()
    print "getting valid column values"
    column_values = {}
    random.shuffle(all_columns)   # helps be fast in parallel
    for c in all_columns:
        column_values[c] = get_column_values(c)
    print u"done, took {} seconds".format(elapsed(start_time))

    while True:
        num_columns = random.randint(1,4)
        chosen_columns = random.sample(all_columns, num_columns)

        # chosen_columns = ["has_green", "state"]
        # print num_columns, chosen_columns
        join_with_a = any([c.startswith(u"a.") for c in chosen_columns])

        join_clause = u" "
        if join_with_a:
            join_clause += u" join mag_authors_paperid a on v.doi=a.doi "

        if chosen_columns:
            where_clause = u" AND ".join(u"({}={})".format(c, random.choice(column_values[c])) for c in chosen_columns)
        else:
            where_clause = u" TRUE"

        timing = {}
        start_time = time()
        with get_db_cursor() as cursor:
            timing["0. in with"] = elapsed(start_time)

            start_time = time()
            q = u"select count(distinct v.doi) from ricks_temp_pub_affil_journals2 v {} where {}".format(join_clause, where_clause)

            cursor.execute(q)
            timing["1. after execute"] = elapsed(start_time)

            start_time = time()
            rows = cursor.fetchall()
            timing["2. after fetchall"] = elapsed(start_time)

            print u"{:>10}s {:>15,} values:  {}".format(timing["1. after execute"], rows[0]["count"], where_clause)

