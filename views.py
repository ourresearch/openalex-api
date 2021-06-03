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

from app import app
from app import db
from app import get_db_connection
from app import get_db_cursor
from app import logger
from data.funders import funder_names
from journal import Journal
from topic import Topic
from institution import Institution
from geo import get_geo_rows
from geo import get_oa_from_redshift
from geo import get_oa_from_redshift_fast
from geo import get_all_rows_fast
from transformative_agreement import TransformativeAgreement
from util import str2bool
from util import normalize_title
from util import clean_doi
from util import is_doi
from util import is_issn
from util import get_sql_answer
from util import jsonify_fast
from util import find_normalized_license
from util import str2bool
from util import jsonify_fast_no_sort
from util import NotJournalArticleException
from util import NoDoiException




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
    return jsonify_fast({
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


@app.route("/journal/<issnl_query>", methods=["GET"])
def journal_issnl_get(issnl_query):
    funder_id = request.args.get("funder", None)
    institution_id = request.args.get("institution", None)
    if institution_id and "grid" in institution_id:
        institution = Institution.query.get(institution_id)
    else:
        institution = None

    my_journal = Journal.query.filter(Journal.issnl == issnl_query).first()
    return jsonify(my_journal.to_dict_full(funder_id, institution))


@app.route("/topic/<topic_query>", methods=["GET"])
def topic_get(topic_query):
    funder_id = request.args.get("funder", None)
    institution_id = request.args.get("institution", None)
    if institution_id and "grid" in institution_id:
        institution = Institution.query.get(institution_id)
    else:
        institution = None

    include_uncompliant = False
    if "include-uncompliant" in request.args:
        include_uncompliant = str2bool(request.args.get("include-uncompliant", "true"))
        if request.args.get("include-uncompliant") == '':
            include_uncompliant = True

    if include_uncompliant:
        limit = 50  # won't need to filter any out
    else:
        limit = 1000

    topic_hits = Topic.query.filter(Topic.topic == topic_query).order_by(Topic.num_articles_3years.desc()).limit(limit)
    our_journals = Journal.query.filter(Journal.issnl.in_([t.issnl for t in topic_hits])).all()
    responses = []
    for this_journal in our_journals:
        if include_uncompliant or this_journal.is_compliant(funder_id, institution):
            response = this_journal.to_dict_journal_row(funder_id, institution)
            responses.append(response)
    responses = sorted(responses, key=lambda k: k['num_articles_since_2018'], reverse=True)[:50]
    return jsonify({ "list": responses, "count": len(responses)})



@app.route("/search/journals/<journal_query>", methods=["GET"])
def search_journals_get(journal_query):
    funder_id = request.args.get("funder", None)
    institution_id = request.args.get("institution", None)
    if institution_id and "grid" in institution_id:
        institution = Institution.query.get(institution_id)
    else:
        institution = None

    include_uncompliant = False
    if "include-uncompliant" in request.args:
        include_uncompliant = str2bool(request.args.get("include-uncompliant", "true"))
        if request.args.get("include-uncompliant") == '':
            include_uncompliant = True

    if include_uncompliant:
        limit = 50  # won't need to filter any out
    else:
        limit = 1000

    response = []

    query_for_search = re.sub(r'[!\'()|&]', ' ', journal_query).strip()
    if query_for_search:
        query_for_search = re.sub(r'\s+', ' & ', query_for_search)
        query_for_search += ':*'

    command = """select 
                issnl, 
                ts_rank_cd(to_tsvector('only_stop_words', title), query, 1) AS rank,
                num_articles + 10000 * ts_rank_cd(to_tsvector('only_stop_words', title), query, 1) as score
            from bq_our_journals_issnl, to_tsquery('only_stop_words', '{query_for_search}') query
            where to_tsvector('only_stop_words', title) @@ query
            order by num_articles_since_2018 + 10000 * ts_rank_cd(to_tsvector('only_stop_words', title), query, 1) desc
            limit {limit}
    """.format(query_for_search=query_for_search, limit=limit)
    res = db.session.connection().execute(sql.text(command))
    rows = res.fetchall()

    issnls = [row[0] for row in rows]
    our_journals = Journal.query.filter(Journal.issnl.in_(issnls)).all()
    # print our_journals
    responses = []
    for this_journal in our_journals:
        if include_uncompliant or this_journal.is_compliant(funder_id, institution):
            response = this_journal.to_dict_journal_row(funder_id, institution)
            matching_score_row = [row for row in rows if row[0]==this_journal.issnl][0]
            response["fulltext_rank"] = matching_score_row[1]
            response["score"] = matching_score_row[2]
            responses.append(response)

    responses = sorted(responses, key=lambda k: k['score'], reverse=True)[:50]

    return jsonify({ "list": responses, "count": len(responses)})


def get_subscription_rows(package="cdl_elsevier"):

    command = "select * from ricks_unpaywall_journals_subscription_agg where package_id = %s"

    with get_db_cursor() as cursor:
        cursor.execute(command, (package,))
        rows = cursor.fetchall()
    return rows

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

def get_subscriptions(package):
    responses = []
    rows = get_subscription_rows(package)


    for row in rows:
        my_dict = {
            "issnl": row["journal_issn_l"],
            "journal_issn_l": row["journal_issn_l"],
            "journal_name": row["title"],
            # "publisher": row["publisher"],
            "affected_start_date": row["from_date"],
            "affected_end_date": row["to_date"],
            "num_dois": row["num_papers"],
            "num_oa": row["num_is_oa"],
            "proportion_publisher_hosted": round(float(row["num_publisher_hosted"]) / row["num_papers"], 4),
            "proportion_repository_hosted": round(float(row["num_repository_hosted"]) / row["num_papers"], 4),
            "proportion_oa": round(float(row["num_is_oa"]) / row["num_papers"], 4),
            "issns": json.loads(row["issns"]),
            "score": row["num_papers"]
        }
        if my_dict["affected_start_date"]:
            if my_dict["affected_start_date"].isoformat()[0:10].endswith('12-31'):
                my_dict["affected_start_date"] = my_dict["affected_start_date"] + datetime.timedelta(days=1)
            my_dict["affected_start_date"] = my_dict["affected_start_date"].isoformat()[0:10]
        if my_dict["affected_end_date"]:
            my_dict["affected_end_date"] = my_dict["affected_end_date"].isoformat()[0:10]
        if package == "mit_elsevier":
            my_dict.update({
            "closed_access_downloads": display_closed_access_downloads(row),
            "downloads": display_downloads(row),
            "num_citations": row["mit_num_citations"] if row["mit_num_citations"] else 0,
            })

        responses.append(my_dict)

    responses = sorted(responses, key=lambda k: k['score'], reverse=True)
    return responses


@app.route("/subscriptions.csv", methods=["GET"])
def unpaywall_journals_subscriptions_csv():
    package = request.args.get("package", "cdl_elsevier")

    def csv_value(subscription, key):
        if key == "issns":
            return " " + ";".join(subscription[key]) #need to prefix with space or excel interprets some issns as a date
        if key == "issn_l":
            return " {}".format(subscription[key])  #need to prefix with space or excel interprets some issns as a date
        if "proportion" in key:
            return round(subscription[key], 4)
        return subscription[key]

    subscriptions = get_subscriptions(package)

    filename = "subscriptions.csv"
    with open(filename, "w") as file:
        csv_file = csv.writer(file, encoding='utf-8')
        keys = [k for k in sorted(subscriptions[0].keys()) if k != 'score']
        csv_file.writerow(keys)
        for subscription in subscriptions:
            csv_file.writerow([csv_value(subscription, k) for k in keys])

    with open(filename, "r") as file:
        contents = file.readlines()

    # return Response(contents, mimetype="text/text")
    return Response(contents, mimetype="text/csv")


@app.route("/subscriptions", methods=["GET"])
def unpaywall_journals_subscriptions_get():
    package = request.args.get("package", "cdl_elsevier")
    responses = get_subscriptions(package)
    return jsonify({ "list": responses, "count": len(responses)})

@app.route("/subscriptions/name/<q>", methods=["GET"])
def unpaywall_journals_autocomplete_journals(q):
    package = request.args.get("package", "cdl_elsevier")
    responses = get_subscriptions(package)
    filtered_responses = []
    for response in responses:
        if to_unicode_or_bust(q).lower() in to_unicode_or_bust(response["journal_name"]).lower():
            filtered_responses.append(response)
    return jsonify({ "list": filtered_responses, "count": len(filtered_responses)})

@app.route("/subscription/issn/<q>", methods=["GET"])
def unpaywall_journals_issn(q):
    package = request.args.get("package", "cdl_elsevier")
    responses = get_subscriptions(package)
    for response in responses:
        if to_unicode_or_bust(q).lower() in response["issns"]:
            return jsonify(response)
    abort_json(404, "issn not found in this subscription package")


@app.route("/breakdown", methods=["GET"])
def unpaywall_journals_breakdown():
    package = request.args.get("package", "cdl_elsevier")
    rows = get_subscription_rows(package)
    response = {
        "article_breakdown": {
            "num_closed": sum([r["num_papers"] - r["num_is_oa"] for r in rows]),
            "num_has_repository_hosted_and_has_publisher_hosted": sum([r["num_has_repository_hosted_and_has_publisher_hosted"] for r in rows]),
            "num_has_repository_hosted_and_not_publisher_hosted": sum([r["num_has_repository_hosted_and_not_publisher_hosted"] for r in rows]),
            "num_not_repository_hosted_and_has_publisher_hosted": sum([r["num_not_repository_hosted_and_has_publisher_hosted"] for r in rows])
        },
        "num_articles_total": sum([r["num_papers"] for r in rows]),
        "num_journals_total": len(rows),
    }
    return jsonify(response)

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


def get_total_count(package):

    command = """
            select count(doi) as num_articles
            from unpaywall_production u
            join ricks_unpaywall_journals_subscription_agg j on u.journal_issn_l = j.journal_issn_l
            where 
            package_id = '{package}' and
            u.published_date >= coalesce(j.from_date, '1900-01-01'::timestamp) and u.published_date < coalesce(j.to_date, '2100-01-01'::timestamp)
            {text_filter}
            {oa_filter}
        """.format(text_filter=build_text_filter(),
                   package=package,
                   oa_filter=build_oa_filter())

    # print command
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchone()  # just get first row

    return rows["num_articles"]


@app.route("/articles", methods=["GET"])
def unpaywall_journals_articles_paged():
    package = request.args.get("package", "cdl_elsevier")
    print(package)

    # page starts at 1 not 0
    if request.args.get("page"):
        page = int(request.args.get("page"))
    else:
        page = 1

    if request.args.get("pagesize"):
        pagesize = int(request.args.get("pagesize"))
    else:
        pagesize = 20
    if pagesize > 1000:
        abort_json(400, "pagesize too large; max 1000")

    offset = (page - 1) * pagesize

    command = """
        select usimple.doi, api_json 
        from unpaywall_simple_sortkey usimple, 
        (   select doi
            from unpaywall_production u
            join ricks_unpaywall_journals_subscription_agg j on u.journal_issn_l = j.journal_issn_l
            where 
                package_id = '{package}' and
                u.published_date >= coalesce(j.from_date, '1900-01-01'::timestamp) and u.published_date < coalesce(j.to_date, '2100-01-01'::timestamp) 
                {text_filter}
                {oa_filter}
            order by published_date desc
            limit {pagesize}
            offset {offset}) as s
        where usimple.doi=s.doi    
    """.format(pagesize=pagesize,
                   offset=offset,
                   package=package,
                   text_filter=build_text_filter(),
                   oa_filter=build_oa_filter())
    # print command
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    responses = [json.loads(row["api_json"]) for row in rows]

    return jsonify({"page": page, "list": responses, "total_count": get_total_count(package)})




@newrelic.agent.function_trace()
def get_oa_from_redshift_country():
    return get_oa_from_redshift("country")

@newrelic.agent.function_trace()
def get_oa_from_redshift_subcontinent():
    return get_oa_from_redshift("subcontinent")

@newrelic.agent.function_trace()
def get_oa_from_redshift_continent():
    return get_oa_from_redshift("continent")

@newrelic.agent.function_trace()
def get_oa_from_redshift_global():
    return get_oa_from_redshift("global")


@app.route("/metrics/geo", methods=["GET"])
@newrelic.agent.function_trace()
def metrics_oa_geo_hack_for_subcontinents():

    groupby = request.args.get("groupby", "country")
    if groupby == "country":
        groupby = "subcontinent_as_country"
    get_oa_newrelic_wrapper = newrelic.agent.FunctionTraceWrapper(
        get_oa_from_redshift_fast, name=groupby, group='get_oa_from_redshift')
    (response, timing) = get_oa_newrelic_wrapper(groupby)
    return jsonify_fast({"_timing": timing, "response": response})

@app.route("/metrics/geo_real", methods=["GET"])
@newrelic.agent.function_trace()
def metrics_oa_geo_fast():

    groupby = request.args.get("groupby", "country")
    get_oa_newrelic_wrapper = newrelic.agent.FunctionTraceWrapper(
        get_oa_from_redshift_fast, name=groupby, group='get_oa_from_redshift')
    (response, timing) = get_oa_newrelic_wrapper(groupby)
    return jsonify_fast({"_timing": timing, "response": response})

@app.route("/metrics/geo_all", methods=["GET"])
@newrelic.agent.function_trace()
def metrics_oa_geo_all_as_csv():

    all_response_values = []
    for level in ["country", "subcontinent", "continent", "global"]:

        undefer_column = '*'
        (rows, timing) = get_all_rows_fast(level, undefer_column)
        for row in rows:
            if row.get(level, "global"):
                row["bronze_gold_green_hybrid"] = row["is_oa"]
                row["gold_green_hybrid"] = row["green_gold_hybrid"]
                del(row["green_gold_hybrid"])
                row["bronze_gold_green"] = row["bronze_green_gold"]
                del(row["bronze_green_gold"])
                row["closed"] = row["num_distinct_articles"] - row["is_oa"]
                row["year"] = int(row["year"])
                row["continent"] = row.get("continent", None)
                row["subcontinent"] = row.get("subcontinent", None)
                row["name"] = row.get(level, "global")
                row["iso2"] = row.get("country_iso2", None)
                row["id"] = row.get("country_iso3", hashlib.md5(row["name"].encode()).hexdigest()[0:4])
                row["level"] = level
                all_response_values.append(row)

    keys = list(rows[0].keys())
    keys.reverse()  # a bit nicer this way
    values = [[r[k] for k in keys] for r in all_response_values]  # do it this way to make sure they are in order
    return jsonify_fast({"_timing": timing, "response": {"keys": keys, "values": values}})

@app.route("/metrics/map/continent", methods=["GET"])
@newrelic.agent.function_trace()
def metrics_continent_map():
    with open("data/world-continents.json") as f:
        data = f.read()
    return Response(data, mimetype="application/json")

@app.route("/metrics/map/country", methods=["GET"])
@newrelic.agent.function_trace()
def metrics_country_map():
    with open("data/world-countries-sans-antarctica.json") as f:
        data = f.read()
    return Response(data, mimetype="application/json")

@app.route("/metrics/iso2_to_iso3", methods=["GET"])
@newrelic.agent.function_trace()
def metrics_iso2_to_iso3():

    all_response_values = {}
    (rows, timing) = get_all_rows_fast("country", "country_iso2, country_iso3")
    for row in rows:
        all_response_values[row["country_iso2"]] = row["country_iso3"]

    return jsonify_fast({"_timing": timing, "response": all_response_values})

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


def get_affiliation_rows_from_doi(dirty_doi):
    my_doi = clean_doi(dirty_doi)
    command = "select * from mag_doi_affiliations_details_view where doi='{}'".format(my_doi)
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    return rows

def get_citation_from_crossref(dirty_doi):
    my_doi = clean_doi(dirty_doi)
    headers = {"Accept": "text/bibliography; style=cell; locale=en-US"}
    r = requests.get("https://doi.org/{}".format(my_doi), headers=headers)
    if r.status_code == 200:
        my_citation = r.content.decode('utf-8').strip()
        return "[{}]".format(my_citation)
    return "https://doi.org/{}".format(my_doi)

def get_citation_elements_from_crossref(dirty_doi):
    my_doi = clean_doi(dirty_doi)
    headers = {"Accept": "application/json", "User-Agent": "team@ourresearch.org"}
    r = requests.get("https://api.crossref.org/works/{}".format(my_doi), headers=headers)
    if r.status_code == 200:
        data = r.json()["message"]
        try:
            author = data["author"][0]["family"]
        except:
            author = ""
        response = {
            "volume": data.get("volume", ""),
            "issue": data.get("issue", ""),
            "pages": data.get("page", ""),
            "container_title": data.get("container_title", [""])[0],
            "article_title": data["title"][0],
            "author": author
        }
        return response
    return {}


def get_standard_versions(dirty_list):
    if not dirty_list:
        return []

    lookup = {
        "preprint": "submittedVersion",
        "postprint": "acceptedVersion",
        "publisher pdf": "publishedVersion"
    }
    return [lookup.get(v.lower(), v.lower()) for v in dirty_list if v]


@app.route("/jump/temp/package/<package>", methods=["GET"])
def jump_package_get(package):
    command = """select issn_l, journal_name from unpaywall_journals_package_issnl_view where package='{}'""".format(package)

    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()

    return jsonify({"list": rows, "count": len(rows)})


@app.route("/jump/temp/issn/<issn_l>", methods=["GET"])
def jump_issn_get(issn_l):
    use_cache = str2bool(request.args.get("use_cache", "true"))
    package = request.args.get("package", "demo")
    if package == "demo":
        package = "uva_elsevier"
    min_arg = request.args.get("min", None)

    if use_cache:
        jump_response = jump_cache[package]
    else:
        jump_response = get_jump_response(package, min_arg)

    journal_dicts = jump_response["list"]
    issnl_dict = filter(lambda my_dict: my_dict['issn_l'] == issn_l, journal_dicts)[0]

    command = """select year, oa_status, count(*) as num_articles from unpaywall 
    where journal_issn_l = '{}'
    and year > 2015
    group by year, oa_status""".format(issn_l)

    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    for row in rows:
        row["year"] = int(row["year"])

    issnl_dict["oa_status"] = rows

    return jsonify(issnl_dict)


def get_issn_ls_for_package(package):
    command = "select issn_l from unpaywall_journals_package_issnl_view"
    if package:
        command += " where package='{}'".format(package)
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    package_issn_ls = [row["issn_l"] for row in rows]
    return package_issn_ls


@app.route("/jump/temp", methods=["GET"])
def jump_get():
    use_cache = str2bool(request.args.get("use_cache", "true"))
    package = request.args.get("package", "demo")
    if package == "demo":
        package = "uva_elsevier"
    min_arg = request.args.get("min", None)

    if use_cache:
        global jump_cache
        return jsonify_fast(jump_cache[package])
    else:
        return jsonify_fast(get_jump_response(package, min_arg))

def get_jump_response(package="mit_elsevier", min_arg=None):
    timing = []

    start_time = time()
    section_time = time()

    package_issn_ls = get_issn_ls_for_package(package)

    command = "select * from counter where package='{}'".format(package)
    counter_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        counter_rows = cursor.fetchall()
    counter_dict = dict((a["issn_l"], a["total"]) for a in counter_rows)

    command = "select * from journal_delayed_oa_active"
    embargo_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        embargo_rows = cursor.fetchall()
    embargo_dict = dict((a["issn_l"], int(a["embargo"])) for a in embargo_rows)

    command = """select cites.journal_issn_l, sum(num_citations) as num_citations_2018
        from ricks_temp_num_cites_by_uva cites
        join unpaywall u on u.doi=cites.doi
        join unpaywall_journals_package_issnl_view package on package.issn_l=cites.journal_issn_l
        where year = 2018
        and u.publisher ilike 'elsevier%'
        and package = '{}'
        group by cites.journal_issn_l""".format(package)
    citation_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        citation_rows = cursor.fetchall()
    citation_dict = dict((a["journal_issn_l"], a["num_citations_2018"]) for a in citation_rows)

    command = """select u.journal_issn_l as journal_issn_l, count(u.doi) as num_authorships
        from unpaywall u 
        join ricks_affiliation affil on u.doi = affil.doi
        where affil.org = 'University of Virginia'
        and u.year = 2018
        group by u.journal_issn_l""".format(package)
    authorship_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        authorship_rows = cursor.fetchall()
    authorship_dict = dict((a["journal_issn_l"], a["num_authorships"]) for a in authorship_rows)


    command = "select * from jump_elsevier_unpaywall_downloads"
    jump_elsevier_unpaywall_downloads_rows = None
    with get_db_cursor() as cursor:
        cursor.execute(command)
        jump_elsevier_unpaywall_downloads_rows = cursor.fetchall()

    timing.append(("time from db", elapsed(section_time, 2)))
    section_time = time()

    rows_to_export = []
    summary_dict = {}
    summary_dict["year"] = [2020 + projected_year for projected_year in range(0, 5)]
    for field in ["total", "oa", "researchgate", "back_catalog", "turnaways"]:
        summary_dict[field] = [0 for projected_year in range(0, 5)]

    timing.append(("summary", elapsed(section_time, 2)))
    section_time = time()

    for row in jump_elsevier_unpaywall_downloads_rows:
        if package and row["issn_l"] not in package_issn_ls:
            continue

        my_dict = {}
        for field in list(row.keys()):
            if not row[field]:
                row[field] = 0

        for field in ["issn_l", "title", "subject", "publisher"]:
            my_dict[field] = row[field]
        my_dict["papers_2018"] = row["num_papers_2018"]
        my_dict["citations_from_mit_in_2018"] = citation_dict.get(my_dict["issn_l"], 0)
        my_dict["num_citations"] = citation_dict.get(my_dict["issn_l"], 0)
        my_dict["num_authorships"] = authorship_dict.get(my_dict["issn_l"], 0)
        my_dict["oa_embargo_months"] = embargo_dict.get(my_dict["issn_l"], None)

        my_dict["downloads_by_year"] = {}
        my_dict["downloads_by_year"]["year"] = [2020 + projected_year for projected_year in range(0, 5)]

        oa_recall_scaling_factor = 1.3
        researchgate_proportion_of_downloads = 0.1
        growth_scaling = {}
        growth_scaling["downloads"] =   [1.10, 1.21, 1.34, 1.49, 1.65]
        growth_scaling["oa"] =          [1.16, 1.24, 1.57, 1.83, 2.12]
        my_dict["downloads_by_year"]["total"] = [row["downloads_total"]*growth_scaling["downloads"][year] for year in range(0, 5)]
        my_dict["downloads_by_year"]["oa"] = [int(oa_recall_scaling_factor * row["downloads_total_oa"] * growth_scaling["oa"][year]) for year in range(0, 5)]

        my_dict["downloads_by_year"]["oa"] = [min(a, b) for a, b in zip(my_dict["downloads_by_year"]["total"], my_dict["downloads_by_year"]["oa"])]

        my_dict["downloads_by_year"]["researchgate"] = [int(researchgate_proportion_of_downloads * my_dict["downloads_by_year"]["total"][projected_year]) for projected_year in range(0, 5)]
        my_dict["downloads_by_year"]["researchgate_orig"] = my_dict["downloads_by_year"]["researchgate"]

        total_downloads_by_age = [row["downloads_{}y".format(age)] for age in range(0, 5)]
        oa_downloads_by_age = [row["downloads_{}y_oa".format(age)] for age in range(0, 5)]

        my_dict["downloads_by_year"]["turnaways"] = [0 for year in range(0, 5)]
        for year in range(0,5):
            my_dict["downloads_by_year"]["turnaways"][year] = (1 - researchgate_proportion_of_downloads) *\
                sum([(total_downloads_by_age[age]*growth_scaling["downloads"][year] - oa_downloads_by_age[age]*growth_scaling["oa"][year])
                     for age in range(0, year+1)])
        my_dict["downloads_by_year"]["turnaways"] = [max(0, num) for num in my_dict["downloads_by_year"]["turnaways"]]

        my_dict["downloads_by_year"]["oa"] = [min(my_dict["downloads_by_year"]["total"][year] - my_dict["downloads_by_year"]["turnaways"][year], my_dict["downloads_by_year"]["oa"][year]) for year in range(0,5)]

        my_dict["downloads_by_year"]["back_catalog"] = [my_dict["downloads_by_year"]["total"][projected_year]\
                                                        - (my_dict["downloads_by_year"]["turnaways"][projected_year]
                                                           + my_dict["downloads_by_year"]["oa"][projected_year]
                                                           + my_dict["downloads_by_year"]["researchgate"][projected_year])\
                                                        for projected_year in range(0, 5)]
        my_dict["downloads_by_year"]["back_catalog"] = [max(0, num) for num in my_dict["downloads_by_year"]["back_catalog"]]


        # now scale for the org
        try:
            total_org_downloads = counter_dict[row["issn_l"]]
            total_org_downloads_multiple = total_org_downloads / row["downloads_total"]
        except:
            total_org_downloads_multiple = 0

        for field in ["total", "oa", "researchgate", "back_catalog", "turnaways"]:
            for projected_year in range(0, 5):
                my_dict["downloads_by_year"][field][projected_year] *= float(total_org_downloads_multiple)
                my_dict["downloads_by_year"][field][projected_year] = int(my_dict["downloads_by_year"][field][projected_year])


        for field in ["total", "oa", "researchgate", "back_catalog", "turnaways"]:
            for projected_year in range(0, 5):
                summary_dict[field][projected_year] += my_dict["downloads_by_year"][field][projected_year]

        if min_arg:
            del my_dict["downloads_by_year"]

        my_dict["dollars_2018_subscription"] = float(row["usa_usd"])
        rows_to_export.append(my_dict)

    timing.append(("loop", elapsed(section_time, 2)))
    section_time = time()

    sorted_rows = sorted(rows_to_export, key=lambda x: x["downloads_by_year"]["total"][0], reverse=True)
    timing.append(("after sort", elapsed(section_time, 2)))

    timing_messages = ["{}: {}s".format(*item) for item in timing]
    return {"_timing": timing_messages, "list": sorted_rows, "total": summary_dict, "count": len(sorted_rows)}
#
# jump_cache = {}
# store_cache = False
# if store_cache:
#     print "building cache"
#     for package in ["cdl_elsevier", "mit_elsevier", "uva_elsevier"]:
#         print package
#         jump_cache[package] = get_jump_response(package)
#         pickle.dump(jump_cache, open( "data/jump_cache.pkl", "wb" ), -1)
#     print "done"
# else:
#     print "loading cache"
#     jump_cache = pickle.load(open( "data/jump_cache.pkl", "rb" ))



if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5003))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)




# PATH=$(pyenv root)/shims:$PATH
# echo 'PATH=$(pyenv root)/shims:$PATH' >> ~/.zshrc
# /Users/hpiwowar/.pyenv/versions/3.9.5/bin/python3
# PYTHONPATH=/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/site-packages:








