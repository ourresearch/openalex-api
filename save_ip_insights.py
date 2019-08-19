from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import text
from executor import execute
import requests
from time import time
import datetime
import shortuuid
from urllib import quote
import os
import re
import json
import geoip2.webservice

from app import logger
from app import db
from app import app
from util import elapsed
from util import safe_commit
from app import get_db_cursor
from app import get_db_connection

def get_unpaywall_events(chunk=25):
    insights_client = geoip2.webservice.Client(os.getenv("MAXMIND_CLIENT_ID"), os.getenv("MAXMIND_API_KEY"))

    # writing into database

    command = """select ip from papertrail_unpaywall_extracted 
              where ip not in (select ip from unpaywall_ip_lookup) 
              order by received_at_raw desc 
              limit {chunk}""".format(chunk=chunk)

    # print command
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()

    ips = [row["ip"] for row in rows]
    num_since_commit = 0
    insight_objs = []
    for ip in ips:
        ip_to_lookup = ip.split(",")[0]  # sometimes has two separated by comma for some reason
        print ip

        try:
            response_insights = insights_client.insights(ip_to_lookup)
        except (ValueError, geoip2.errors.AddressNotFoundError):
            # this is what it throws if bad ip address
            response_insights = None

        if response_insights:
            insight_dict = response_insights.raw
            for key in ["city", "country", "continent", "registered_country"]:
                if key in insight_dict and  "names" in insight_dict[key]:
                    insight_dict[key]["name"] = insight_dict[key]["names"]["en"]
                    del insight_dict[key]["names"]
            for key in ["subdivisions"]:
                if key in insight_dict:
                    my_list = []
                    for item in insight_dict[key]:
                        if "names" in item:
                            item["name"] = item["names"]["en"]
                            del item["names"]
                    my_list.append(item)
                    insight_dict[key] = my_list
            insight_objs.append(IpInsights(ip=ip,
                                  organization=insight_dict["traits"].get("organization", None),
                                  user_type=insight_dict["traits"].get("user_type", None),
                                  insights=json.dumps(insight_dict)))
        else:
            insight_objs.append(IpInsights(ip=ip,
                                  organization=None,
                                  user_type=None,
                                  insights=""))


    print "committing"
    with get_db_cursor() as cursor:
        command = u"""INSERT INTO unpaywall_ip_lookup (ip, updated, organization, user_type, insights) values """
        insert_strings = []
        for obj in insight_objs:
            if obj.organization:
                obj.organization = obj.organization.replace("'", "''")
            if obj.user_type:
                obj.user_type = obj.user_type.replace("'", "''")
            if obj.insights:
                obj.insights = obj.insights.replace("'", "''")
            insert_string = u"""('{}', '{}', '{}', '{}', '{}')""".format(
                obj.ip, obj.updated, obj.organization, obj.user_type, obj.insights)
            insert_strings.append(insert_string)
        command = command + u",".join(insert_strings) + u";"
        # print command
        cursor.execute(command)


class IpInsights(db.Model):
    __tablename__ = 'unpaywall_ip_lookup'
    __bind_key__ = "redshift_db"

    ip = db.Column(db.Text, primary_key=True)
    updated = db.Column(db.DateTime)
    insights = db.Column(JSONB)
    user_type = db.Column(db.Text)
    organization = db.Column(db.Text)

    def __init__(self, **kwargs):
        self.updated = datetime.datetime.utcnow().isoformat()
        super(IpInsights, self).__init__(**kwargs)

    def __repr__(self):
        return u"{} ({}) {}, {})".format(self.__class__.__name__, self.ip, self.user_type, self.organization)

if __name__ == "__main__":

    start = time()

    while True:
        get_unpaywall_events()
        print "*",