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

all_columns = """doi
is_oa
has_hybrid
has_green
has_gold
has_bronze
is_in_doaj
journal
publisher
year
city
state
country
continent
subcontinent""".split("\n")


def get_column_values(column):
    with get_db_cursor() as cursor:
        q = "select {} from ricks_temp_pub_affil_journals2 where {} is not null order by random() limit 1000".format(column, column)
        cursor.execute(q)
        rows = cursor.fetchall()
    values = []
    for row in rows:
        if isinstance(row[column], bool) or isinstance(row[column], int) or isinstance(row[column], long):
            values.append(row[column])
        else:
            values.append(u"'{}'".format(row[column].decode('utf-8')))
    return values


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")
    parser.add_argument('--pg', nargs="?", type=str, help="table name in postgres (eg bq_journals)")
    parser.add_argument('--bq', nargs="?", type=str, help="table name in bigquery (eg unpaywall.journals)")

    parsed_args = parser.parse_args()

    start_time = time()
    print "getting valid column values"
    column_values = {}
    for c in all_columns:
        column_values[c] = get_column_values(c)
    print u"done, took {} seconds".format(elapsed(start_time))

    while True:
        num_columns = random.randint(1,4)
        chosen_columns = random.sample(all_columns, num_columns)

        # chosen_columns = ["has_green", "state"]
        # print num_columns, chosen_columns

        if chosen_columns:
            where_clause = u" AND ".join(u"({}={})".format(c, random.choice(column_values[c])) for c in chosen_columns)
        else:
            where_clause = u" TRUE"

        timing = {}
        start_time = time()
        with get_db_cursor() as cursor:
            timing["0. in with"] = elapsed(start_time)

            start_time = time()
            q = u"select count(distinct doi) from ricks_temp_pub_affil_journals2 where {}".format(where_clause)

            cursor.execute(q)
            timing["1. after execute"] = elapsed(start_time)

            start_time = time()
            rows = cursor.fetchall()
            timing["2. after fetchall"] = elapsed(start_time)

            print u"{:>10}s {:>15,} values:  {}".format(timing["1. after execute"], rows[0]["count"], where_clause)

