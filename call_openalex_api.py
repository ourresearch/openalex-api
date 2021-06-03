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

all_columns = ['v.doi', 'v.is_oa', 'v.has_hybrid', 'v.has_green', 'v.has_gold', 'v.has_bronze',
               'v.issn_l', 'v.publisher', 'v.org', 'v.year', 'v.city', 'v.state', 'v.country',
               'v.continent', 'v.subcontinent', 'a.normalized_name']

# number of combos of length up to four
# is 15 choose 4 + 15 choose 3 + ... 15 choose 1  https://www.calculatorsoup.com/calculators/discretemathematics/combinations.php?n=15&r=4&action=solve
# = 1365 + 455 + 105 + 15
# = 1940
# 3.5 seconds per = 3.5*1940 = 6790 seconds = 1.9 hours until everything has been primed once

def get_column_values(column, random=False):
    print("getting values for column {}".format(column))
    (column_table, column_solo) = column.split(".")
    if (column_table == "a"):
        table = "mag_authors_paperid3 a"
    else:
        table = "ricks_fast_pub_affil_journal v"

    with get_db_cursor() as cursor:
        if random:
            q = "select {column}, count(*) as n from {table} where {column} is not null group by {column} order by random() limit 100".format(
                column=column, table=table)
        else:
            q = "select {column}, count(*) as n from {table} where {column} is not null group by {column} order by n desc limit 100".format(
                column=column, table=table)
        cursor.execute(q)
        rows = cursor.fetchall()

    return rows


def get_column_values_for_querying(column, random=False):
    (column_table, column_solo) = column.split(".")

    rows = get_column_values(column, random)

    values = []
    for row in rows:
        if isinstance(row[column_solo], bool) or isinstance(row[column_solo], int) or isinstance(row[column_solo], int):
            value = row[column_solo]
            values.append(value)
        else:
            # value = row[column_solo].decode('utf-8')
            value = row[column_solo]
            value = value.replace("'", "''")
            if value:
                value = "'{}'".format(value)
                values.append(value)  # don't include empty strings
    return values


def do_query(filters, groupby=None, details=False, details_limit=100, verbose=True):
    timer = Timer()

    # chosen_columns = ["has_green", "state"]
    # print num_columns, chosen_columns
    join_with_a = any([filter.startswith("(a.") for filter in filters])

    join_clause = " "
    if join_with_a:
        join_clause += " JOIN mag_authors_paperid3 a ON v.pub_id=a.pub_id "

    if (len(filters) > 0) and (len(filters[0]) > 0):
        where_clause = " AND ".join(filters)
    else:
        where_clause = " TRUE"

    with get_db_cursor() as cursor:
        timer.log_timing("0. in with")

        if not groupby:
            groupby = 1

        if details:
            q = """SELECT v.*, 1 as n
                    FROM ricks_fast_pub_affil_journal v 
                    {join_clause} 
                    WHERE {where_clause}
                    ORDER BY RANDOM()
                    LIMIT {details_limit}
                    """.format(
                        join_clause=join_clause,
                        where_clause=where_clause,
                        details_limit=details_limit)
        else:
            q = """SELECT {groupby}, count(distinct v.doi) as n 
                    FROM ricks_fast_pub_affil_journal v 
                    {join_clause} 
                    WHERE {where_clause}
                    GROUP BY {groupby} 
                    ORDER BY n DESC
                    """.format(
                        join_clause=join_clause,
                        where_clause=where_clause,
                        groupby=groupby)
        if verbose:
            print(q)

        cursor.execute(q)
        timer.log_timing("1. after execute")

        rows = cursor.fetchall()
        timer.log_timing("2. after fetchall")

        print("{:>10}s {:>15,} rows:  {}, group by {}".format(timer.elapsed_total, len(rows), where_clause, groupby))

        if not details and (groupby == "v.doi"):
            rows = [row["doi"] for row in rows]

        q = re.sub("\s+", ' ', q)
        return (rows, q, timer.to_dict())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")
    parser.add_argument('--warm', action='store_true', help="warm cache")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    chosen_columns_combinations_remaining = []
    if parsed_vars.get("warm"):
        for num_columns in range(1, 5):
            chosen_columns_combinations_remaining += combinations(all_columns, num_columns)
        # print chosen_columns_combinations_remaining
        random.shuffle(chosen_columns_combinations_remaining)

    start_time = time()
    print("getting valid column values")
    column_values = {}
    random.shuffle(all_columns)   # helps be fast in parallel
    for c in all_columns:
        column_values[c] = get_column_values_for_querying(c)
    print("done, took {} seconds".format(elapsed(start_time)))


    keep_running = True
    while keep_running:

        if chosen_columns_combinations_remaining:
            chosen_columns = chosen_columns_combinations_remaining.pop()
            if not chosen_columns_combinations_remaining:
                keep_running = False
        else:
            num_columns = random.randint(1,4)
            chosen_columns = random.sample(all_columns, num_columns)

        filters = ["({}={})".format(c, random.choice(column_values[c])) for c in chosen_columns[1:]]
        groupby = chosen_columns[0]

        do_query(filters, groupby, verbose=False)