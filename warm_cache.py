# coding: utf-8

import os
import sys
import requests
import time
import argparse
from multiprocessing.pool import ThreadPool
import threading
import weakref
from time import sleep
import random

from app import get_db_cursor
from util import elapsed
from util import chunks

def warm_the_cache():
    q = """select doi from unpaywall where genre='journal-article' and year > 2017 order by random() limit 1000"""
    with get_db_cursor() as cursor:
        cursor.execute(q)
        rows = cursor.fetchall()
    dois = [row["doi"] for row in rows]
    chunked_dois = chunks(dois, 10)

    for chunk in chunked_dois:
        start_time = time.time()

        if not hasattr(threading.current_thread(), "_children"):
            threading.current_thread()._children = weakref.WeakKeyDictionary()

        my_thread_pool = ThreadPool(10)

        def cache_it(doi):
            url = "https://api.greenoait.org/permissions/doi/{}".format(doi)
            # 12*30*24*60*60 = 31104000
            headers = {"Cache-Control": "public, max-age=31104000"}
            # sleep part of a second so not all at once
            sleep(random.random())
            r = requests.get(url, headers=headers)
            return r.status_code

        responses = my_thread_pool.imap_unordered(cache_it, chunk)
        my_thread_pool.close()
        my_thread_pool.join()
        my_thread_pool.terminate()
        print(list(responses))
        print("took {} seconds".format(elapsed(start_time)))



# python import_accounts.py ~/Downloads/new_accounts.csv
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")

    parsed_args = parser.parse_args()
    parsed_vars = vars(parsed_args)

    while True:
        warm_the_cache()



