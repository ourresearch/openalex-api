# coding: utf-8

import os
import sys
import requests
import time
import argparse

from app import get_db_cursor
from util import elapsed

def warm_the_cache():
    q = """select doi from unpaywall where genre='journal-article' and year > 2015 order by random() limit 1000"""
    with get_db_cursor() as cursor:
        cursor.execute(q)
        rows = cursor.fetchall()
    for row in rows:
        start_time = time.time()
        doi = row["doi"]
        url = "https://api.greenoait.org/permissions/doi/{}".format(doi)
        # 12*30*24*60*60 = 31104000
        headers = {"Cache-Control": "public, max-age=31104000"}
        r = requests.get(url, headers=headers)


# python import_accounts.py ~/Downloads/new_accounts.csv
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    warm_the_cache()



