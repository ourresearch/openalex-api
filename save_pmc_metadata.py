from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import text
from executor import execute
import requests
from time import time
from time import sleep
import datetime
import shortuuid
from urllib import quote
import os
import re
import json
from dateutil import parser

from sickle import Sickle
from sickle.iterator import OAIItemIterator
from sickle.models import ResumptionToken
from sickle.oaiexceptions import NoRecordsMatch
from sickle.response import OAIResponse

from app import logger
from app import db
from app import app
from util import elapsed
from util import safe_commit
from util import is_ip
from app import get_db_cursor
from app import get_db_connection

import sickle




class PmcPmhDates(db.Model):
    __tablename__ = 'unpaywall_pmc_pmh_dates'
    __bind_key__ = "redshift_db"

    pmh_id = db.Column(db.Text, primary_key=True)
    updated = db.Column(db.DateTime)
    doi = db.Column(db.Text)
    pmh_date = db.Column(db.DateTime)

    def __init__(self, **kwargs):
        self.updated = datetime.datetime.utcnow().isoformat()
        super(PmcPmhDates, self).__init__(**kwargs)

    def __repr__(self):
        return u"{} ({}) {}".format(self.__class__.__name__, self.pmh_id, self.pmh_date)


class MyOAIItemIterator(OAIItemIterator):
    def _get_resumption_token(self):
        """Extract and store the resumptionToken from the last response."""
        resumption_token_element = self.oai_response.xml.find(
            './/' + self.sickle.oai_namespace + 'resumptionToken')
        if resumption_token_element is None:
            return None
        token = resumption_token_element.text
        cursor = resumption_token_element.attrib.get('cursor', None)
        complete_list_size = resumption_token_element.attrib.get(
            'completeListSize', None)
        expiration_date = resumption_token_element.attrib.get(
            'expirationDate', None)
        resumption_token = ResumptionToken(
            token=token, cursor=cursor,
            complete_list_size=complete_list_size,
            expiration_date=expiration_date
        )
        return resumption_token

    def get_complete_list_size(self):
        """Extract and store the resumptionToken from the last response."""
        resumption_token_element = self.oai_response.xml.find(
            './/' + self.sickle.oai_namespace + 'resumptionToken')
        if resumption_token_element is None:
            return None
        complete_list_size = resumption_token_element.attrib.get(
            'completeListSize', None)
        if complete_list_size:
            return int(complete_list_size)
        return complete_list_size


# subclass so we can customize the number of retry seconds
class MySickle(Sickle):
    RETRY_SECONDS = 120

    def get_http_response_url(self):
        if hasattr(self, "http_response_url"):
            return self.http_response_url
        return None

    def harvest(self, **kwargs):  # pragma: no cover
        """Make HTTP requests to the OAI server.
        :param kwargs: OAI HTTP parameters.
        :rtype: :class:`sickle.OAIResponse`
        """
        start_time = time()
        for _ in range(self.max_retries):
            if self.http_method == 'GET':
                payload_str = "&".join("%s=%s" % (k,v) for k,v in kwargs.items())
                url_without_encoding = u"{}?{}".format(self.endpoint, payload_str)
                http_response = requests.get(url_without_encoding,
                                             **self.request_args)
                self.http_response_url = http_response.url
            else:
                http_response = requests.post(self.endpoint, data=kwargs,
                                              **self.request_args)
                self.http_response_url = http_response.url
            if http_response.status_code == 503:
                retry_after = self.RETRY_SECONDS
                logger.info("HTTP 503! Retrying after %d seconds..." % retry_after)
                sleep(retry_after)
            else:
                pass
                # logger.info("took {} seconds to call pmh url: {}".format(elapsed(start_time), http_response.url))

                http_response.raise_for_status()
                if self.encoding:
                    http_response.encoding = self.encoding
                return OAIResponse(http_response, params=kwargs)

def get_pmh_dates(start_date = None):
    my_sickle = None

    if not start_date:
        command = """select max(pmh_date) as max_pmh_date from unpaywall_pmc_pmh_dates"""
        with get_db_cursor() as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()
        if rows[0]["max_pmh_date"]:
            start_date = rows[0]["max_pmh_date"]
        else:
            start_date = parser.parse("2001-02-27")

    new_objects = []

    while True:
        (header, my_sickle, start_date) = call_pmh_endpoint(start_date, my_sickle)  # can iterate up if no records
        xml_content = header.next()
        while xml_content:
            matches = re.findall(ur"<identifier>(.+?)</identifier><datestamp>(.+?)</datestamp>", str(xml_content))
            (pmh_id, pmh_date) = matches[0]
            my_obj = PmcPmhDates(pmh_id=pmh_id, pmh_date=pmh_date)
            print ".",
            new_objects.append(my_obj)

            if len(new_objects) > 500:
                # print "committing"
                with get_db_cursor() as cursor:
                    command = u"""INSERT INTO unpaywall_pmc_pmh_dates (pmh_id, pmh_date, updated) values """
                    insert_strings = []
                    for obj in new_objects:
                        insert_string = u"""('{}', '{}', '{}')""".format(
                            obj.pmh_id, obj.pmh_date, obj.updated)
                        insert_strings.append(insert_string)
                    command = command + u",".join(insert_strings) + u";"
                    print "*",
                    cursor.execute(command)
                new_objects = []

            try:
                xml_content = header.next()
            except:
                xml_content = None
                start_date = start_date + datetime.timedelta(days=1)





def call_pmh_endpoint(start_date, my_sickle=None):

    if not my_sickle:
        proxies = {}
        repo_pmh_url = "https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi"
        my_sickle = MySickle(repo_pmh_url, proxies=proxies, timeout=30, iterator=MyOAIItemIterator)

    args = {}
    args['metadataPrefix'] = 'oai_dc'

    args['from'] = start_date.isoformat()[0:10]
    args["until"] = (start_date + datetime.timedelta(days=1)).isoformat()[0:10]

    try:
        header = my_sickle.ListIdentifiers(ignore_deleted=True, **args)
    except sickle.oaiexceptions.NoRecordsMatch:
        start_date = start_date + datetime.timedelta(days=1)
        return call_pmh_endpoint(start_date, my_sickle)

    print "{}\n".format(start_date.isoformat()[0:10])
    return (header, my_sickle, start_date)


if __name__ == "__main__":

    while True:
        get_pmh_dates()



