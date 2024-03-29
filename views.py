from flask import make_response
from flask import request
from flask import redirect
from flask import abort
from flask import render_template
from flask import jsonify
from flask import g
from flask import Response

import json
import os
import sys
import re
import datetime
from time import time
import boto
import pickle
from util import read_csv_file
from util import elapsed
from util import to_unicode_or_bust
from collections import defaultdict
from sqlalchemy import sql
from sqlalchemy import orm
import newrelic.agent
import psycopg2
import hashlib
import unicodecsv as csv
import dateutil.parser
from monthdelta import monthdelta
import requests
from collections import OrderedDict
import copy
from call_openalex_api import get_column_values
from call_openalex_api import field_lookup
from call_openalex_api import do_query
from call_openalex_api import get_work

from app import app
from app import db
from app import get_db_connection
from app import get_db_cursor
from app import logger
from data.funders import funder_names
from institution import Institution
from util import clean_doi
from util import is_doi
from util import is_issn
from util import jsonify_fast_no_sort
from util import str2bool
from util import Timer





def json_dumper(obj):
    """
    if the obj has a to_dict() function we've implemented, uses it to get dict.
    from http://stackoverflow.com/a/28174796
    """
    try:
        return obj.to_dict()
    except AttributeError:
        return obj.__dict__


def json_resp(thing):
    json_str = json.dumps(thing, sort_keys=True, default=json_dumper, indent=4)

    if request.path.endswith(".json") and (os.getenv("FLASK_DEBUG", False) == "True"):
        logger.info("rendering output through debug_api.html template")
        resp = make_response(render_template(
            'debug_api.html',
            data=json_str))
        resp.mimetype = "text/html"
    else:
        resp = make_response(json_str, 200)
        resp.mimetype = "application/json"
    return resp


def abort_json(status_code, msg):
    body_dict = {
        "HTTP_status_code": status_code,
        "message": msg,
        "error": True
    }
    resp_string = json.dumps(body_dict, sort_keys=True, indent=4)
    resp = make_response(resp_string, status_code)
    resp.mimetype = "application/json"
    abort(resp)



@app.after_request
def after_request_stuff(resp):

    #support CORS
    resp.headers['Access-Control-Allow-Origin'] = "*"
    resp.headers['Access-Control-Allow-Methods'] = "POST, GET, OPTIONS, PUT, DELETE, PATCH"
    resp.headers['Access-Control-Allow-Headers'] = "origin, content-type, accept, x-requested-with"

    # # remove session
    # db.session.remove()

    # without this jason's heroku local buffers forever
    sys.stdout.flush()

    return resp



@app.route('/', methods=["GET", "POST"])
def base_endpoint():
    return jsonify_fast_no_sort({
        "version": "0.0.1",
        "msg": "Welcome to OpenAlex. Don't panic"
    })


@app.route("/autocomplete/topics/name/<q>", methods=["GET"])
def topics_title_search(q):
    ret = []

    query_for_search = re.sub(r'[!\'()|&]', ' ', q).strip()
    if query_for_search:
        query_for_search = re.sub(r'\s+', ' & ', query_for_search)
        query_for_search += ':*'

    command = """with together as (
			select
            topic,
            sum(num_articles_3years) as num_total_3years
        	from bq_scimago_issnl_topics group by topic)
            select 
                topic,
                num_total_3years, 
                ts_rank_cd(to_tsvector('only_stop_words', topic), query, 1) AS rank,
                num_total_3years + 100000 * ts_rank_cd(to_tsvector('only_stop_words', topic), query, 1) as score
            from together, to_tsquery('only_stop_words', '{query_for_search}') query
            where to_tsvector('only_stop_words', topic) @@ query
            order by num_total_3years + 100000 * ts_rank_cd(to_tsvector('only_stop_words', topic), query, 1) desc
            limit 10
        """.format(query_for_search=query_for_search)
    res = db.session.connection().execute(sql.text(command))
    rows = res.fetchall()
    for row in rows:
        ret.append({
            "topic": row[0],
            "num_total_3years": row[1],
            "fulltext_rank": row[2],
            "score": row[3],
        })
    return jsonify({ "list": ret, "count": len(ret)})

@app.route("/autocomplete/journals/name/<q>", methods=["GET"])
def journal_title_search(q):
    ret = []

    query_for_search = re.sub(r'[!\'()|&]', ' ', q).strip()
    if query_for_search:
        query_for_search = re.sub(r'\s+', ' & ', query_for_search)
        query_for_search += ':*'

    command = """select 
                issnl, 
                num_articles_since_2018, 
                title, 
                prop_cc_by_since_2018,
                ts_rank_cd(to_tsvector('only_stop_words', title), query, 1) AS rank,
                num_articles_since_2018 + 10000 * ts_rank_cd(to_tsvector('only_stop_words', title), query, 1) as score
            
            from bq_our_journals_issnl, to_tsquery('only_stop_words', '{query_for_search}') query
            where to_tsvector('only_stop_words', title) @@ query
            order by num_articles_since_2018 + 10000 * ts_rank_cd(to_tsvector('only_stop_words', title), query, 1) desc
            limit 10
    """.format(query_for_search=query_for_search)
    res = db.session.connection().execute(sql.text(command))
    rows = res.fetchall()
    for row in rows:
        ret.append({
            "id": row[0],
            "num_articles_since_2018": row[1],
            "name": row[2],
            "prop_cc_by_since_2018": row[3],
            "fulltext_rank": row[4],
            "score": row[5],
        })
    return jsonify({ "list": ret, "count": len(ret)})


@app.route("/transformative-agreements", methods=["GET"])
def transformative_agreements_get():
    transformative_agreements = TransformativeAgreement.query.all()
    return jsonify({"list": [ta.to_dict_short() for ta in transformative_agreements], "count": len(transformative_agreements)})

@app.route("/transformative-agreement/<id>", methods=["GET"])
def transformative_agreement_lookup(id):
    my_ta = TransformativeAgreement.query.get(id)
    return jsonify(my_ta.to_dict())


@app.route("/institution/<id>", methods=["GET"])
def institution_lookup(id):
    my_institution = Institution.query.filter(Institution.grid_id == id).first()
    return jsonify(my_institution.to_dict())


@app.route("/funder/<id>", methods=["GET"])
def funder_lookup(id):

    matches = [funder for funder in funder_names if str(funder["id"]) == str(id)]
    name = None
    if matches:
        name = matches[0]["name"]

    return jsonify({"id": id, "name": name})


@app.route("/autocomplete/institutions/name/<q>", methods=["GET"])
def institutions_name_autocomplete(q):
    institutions = Institution.query.filter(Institution.org_name.ilike('%{}%'.format(q))).order_by(Institution.num_papers.desc()).limit(10).all()
    return jsonify({"list": [inst.to_dict() for inst in institutions], "count": len(institutions)})


@app.route("/autocomplete/funders/name/<q>", methods=["GET"])
def funders_name_search(q):

    ret = [funder for funder in funder_names if q.lower() in funder["alternate_names"].lower()]

    return jsonify({"list": ret, "count": len(ret)})




def display_closed_access_downloads(row):
    if not row["num_papers"] or not ["num_is_oa"] or not row["mit_counter_age_0y"]:
        return None

    percent_closed = 1 - float(row["num_is_oa"])/row["num_papers"]
    num_downloads = float(row["mit_counter_age_0y"])
    closed_access_downloads = percent_closed * num_downloads
    if closed_access_downloads > 190:
        return "high"
    if closed_access_downloads > 50:
        return "medium"
    return "low"

def display_downloads(row):
    num_downloads = row["mit_counter_age_0y"]
    if num_downloads > 250:
        return "high"
    if num_downloads > 67:
        return "medium"
    return "low"


def build_oa_filter():
    oa_filter = ""
    if request.args.get("oa_host", None):
        oa_host_text = request.args.get("oa_host", "")
        if oa_host_text == "any":
            oa_filter = " and oa_status != 'closed' "
    return oa_filter

def build_text_filter():
    text_filter = ""
    if request.args.get("q", None):
        text_query = request.args.get("q", None)
        if text_query:
            if is_issn(text_query):
                text_filter = " and u.journal_issn_l = '{}' ".format(text_query)
            elif is_doi(text_query):
                text_filter = " and u.doi = '{}' ".format(clean_doi(text_query))
            else:
                text_filter = " and u.title ilike '%{}%' ".format(text_query)
    return text_filter




def controlled_vocab(text):
    if not text:
        return None
    return text.lower()

def split_clean_list(text, use_controlled_vocab=False):
    if not text:
        return []
    my_response = [a.strip() for a in text.split(",") if a]
    my_response = list(set(my_response))
    if use_controlled_vocab:
        my_response = [controlled_vocab(a) for a in my_response]
    try:
        if not is_issn(my_response[0]):
            my_response = [dateutil.parser.parse(a).isoformat()[0:10] for a in my_response]
    except ValueError:
        pass
    return my_response

@app.route("/entity/<path:details>", methods=["GET"])
def redirect_entity_endpoint(details):
    return redirect('https://openalex-guts.herokuapp.com/{}'.format(details, code=302)) # 302 is temporary

@app.route("/works/<id_type>/<path:id>", methods=["GET"])
def works_id_type_get(id_type, id):
    (response, timer_dict) = get_work(id_type, id)
    return jsonify_fast_no_sort({"_timing": timer_dict, "response": response})

@app.route("/<entity>/attribute/list", methods=["GET"])
def entity_attribute_list(entity):
    timer = Timer()
    timer.log_timing("get values")
    return jsonify_fast_no_sort({"_timing": timer.to_dict(), "response": list(field_lookup[entity].keys())})

@app.route("/<entity>/attribute/<attribute>/random", methods=["GET"])
def entity_attribute_random(entity, attribute):
    timer = Timer()
    response = get_column_values(entity, attribute, random=True)
    timer.log_timing("get values")
    return jsonify_fast_no_sort({"_timing": timer.to_dict(), "response": response})

@app.route("/<entity>/attribute/<attribute>/top", methods=["GET"])
def entity_attribute_top(entity, attribute):
    timer = Timer()
    response = get_column_values(entity, attribute, random=False)
    timer.log_timing("get values")
    return jsonify_fast_no_sort({"_timing": timer.to_dict(), "response": response})

@app.route("/<entity>/query", methods=["GET"])
def entity_query(entity):
    filter = request.args.get("filter", "")
    search = request.args.get("search", "")
    groupby = request.args.get("groupby", None)
    format = request.args.get("format", "json")
    limit = max(100, int(request.args.get("limit", 10)))

    queryonly = False
    if "queryonly" in request.args:
        queryonly = True

    details = False
    if "details" in request.args:
        details = True

    if groupby:
        details = False

    filters_list = []
    if filter:
        filters_list = filter.split(",")

    searches_list = []
    if search:
        searches_list = search.split(",")

    (rows, sql, timing) = do_query(entity, filters_list, searches_list, groupby, details, limit=limit, verbose=False, queryonly=queryonly)
    return jsonify_fast_no_sort({"_timing": timing,
                         "query": {"filter": filter,
                                   "search": search,
                                   "groupby": groupby,
                                   "details": details,
                                   "format": format,
                                   "limit": limit},
                         "sql": sql,
                         "response": rows})



if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5003))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)




# PATH=$(pyenv root)/shims:$PATH; unset PYTHONPATH
if False:
    pass
    print("""
    PATH=$(pyenv root)/shims:$PATH; unset PYTHONPATH
    echo 'PATH=$(pyenv root)/shims:$PATH' >> ~/.zshrc
    /Users/hpiwowar/.pyenv/versions/3.9.5/bin/python3  --version
    PYTHONPATH=/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/site-packages:
    python --version
    """)




 # unset PYTHONPATH

