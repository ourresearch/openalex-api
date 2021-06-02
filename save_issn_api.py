from multiprocessing.pool import ThreadPool
import requests
import argparse
import os
from time import time
from time import sleep
import datetime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy import sql
from sqlalchemy import orm

from app import db
from util import elapsed
from util import safe_commit

class JournalMetadataRaw(db.Model):
    issnl = db.Column(db.Text, primary_key=True)
    journal_title = db.Column(db.Text)
    publisher = db.Column(db.Text)
    issns = db.Column(ARRAY(db.Text), server_default='[]')
    api_raw_issn = db.Column(JSONB)
    api_raw_crossref = db.Column(JSONB)

    def __repr__(self):
        return u'<JournalMetadataRaw ({issnl})>'.format(
            issnl=self.issnl
        )


def call_issn_api(query_text):
    if not query_text:
        return None

    response_data = None

    # has to be issn rather than issn-l endpoint because issnl doesn't have the best title
    # see for example Physical Review D https://portal.issn.org/resource/ISSN/2470-0010?format=json
    url_template = u"https://portal.issn.org/resource/ISSN/{}?format=json"

    url = url_template.format(query_text)
    print url
    r = requests.get(url)
    if r.status_code == 200:
        try:
            response_data = r.json()
        except ValueError:
            pass

    # print response_data
    return response_data

def call_crossref_api(query_text):
    if not query_text:
        return None

    response_data = None

    url_template = u"https://api.crossref.org/journals/{}"
    url = url_template.format(query_text)
    print url
    r = requests.get(url)
    if r.status_code == 200:
        try:
            response_data = r.json()
        except ValueError:
            pass

    # print response_data
    return response_data





if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")

    parsed = parser.parse_args()

    start = time()

    while True:
        query = JournalMetadataRaw.query.filter(JournalMetadataRaw.api_raw_crossref == None)\
            .order_by(JournalMetadataRaw.issnl.desc())\
            .limit(10)
        journal_metadata_objs = query.all()

        if not journal_metadata_objs:
            print "done!"
            exit()

        for my_obj in journal_metadata_objs:
            # if not my_obj.api_raw_issn:
            #     result = call_issn_api(my_obj.issnl)
            #     if not result:
            #         result = "ERROR"
            #     my_obj.api_raw_issn = result
            #     sleep(3)

            if not my_obj.api_raw_crossref:
                result = call_crossref_api(my_obj.issnl)
                if not result:
                    result = "ERROR"
                my_obj.api_raw_crossref = result

            db.session.merge(my_obj)

        safe_commit(db)
        print ".",

        db.session.remove()
        # print "finished update in {}sec".format(elapsed(start))
        print "*",

