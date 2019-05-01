from multiprocessing.pool import ThreadPool
import requests
import argparse
import os
from time import time
from time import sleep
import datetime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import sql
from sqlalchemy import orm

from app import db
from util import elapsed
from util import safe_commit

def call_issn_api(query_text):
    if not query_text:
        return None

    response_data = None

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



def store_issn_api(my_queue_save_obj):
    result = call_issn_api(my_queue_save_obj.issnl)
    if not result:
        result = "ERROR"
    my_queue_save_obj.api_raw = result
    db.session.merge(my_queue_save_obj)
    safe_commit(db)
    print ".",
    sleep(3)


class QueueSave(db.Model):
    __tablename__ = "journal_issnl_api"
    issnl = db.Column(db.Text, primary_key=True)
    api_raw = db.Column(JSONB)

    def __repr__(self):
        return u'<QueueSave ({issnl})>'.format(
            issnl=self.issnl
        )



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")

    parsed = parser.parse_args()

    start = time()

    if __name__ == '__main__':
        for i in range(100000):
            query = QueueSave.query.filter(QueueSave.api_raw == None)\
                .order_by(QueueSave.issnl.desc())\
                .limit(1)
            queue_save_objs = query.all()

            for save_obj in queue_save_objs:
                print save_obj
                store_issn_api(save_obj)

            db.session.remove()
            # print "finished update in {}sec".format(elapsed(start))
            print "*",

