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
        logger.info(u"rendering output through debug_api.html template")
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
        "msg": "Don't panic"
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
    institutions = Institution.query.filter(Institution.org_name.ilike(u'%{}%'.format(q))).order_by(Institution.num_papers.desc()).limit(10).all()
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


def get_subscription_rows():
    # command = """with unpaywall_host_type_derived as (
    #         select
    #         journal_issn_l,
    #         published_date,
    #         oa_status,
    #         is_oa='true' as is_oa,
    #         oa_status in ('gold', 'hybrid', 'bronze') as is_publisher_hosted,
    #         has_green as is_repository_hosted
    #         from unpaywall_production u
    #     ) ,
    #     journal_stats as (
    #         select journal_issn_l,
    #         max(cdl.from_date) as from_date,
    #         coalesce(max(cdl.to_date), max(published_date::timestamp)) as to_date,
    #         count(*) as num_papers,
    #         sum(case when is_oa then 1 else 0 end) as num_is_oa,
    #         sum(case when is_publisher_hosted then 1 else 0 end) as num_publisher_hosted,
    #         sum(case when is_repository_hosted then 1 else 0 end) as num_repository_hosted,
    #         sum(case when is_repository_hosted and is_publisher_hosted then 1 else 0 end) as num_has_repository_hosted_and_has_publisher_hosted,
    #         sum(case when is_repository_hosted and not is_publisher_hosted then 1 else 0 end) as num_has_repository_hosted_and_not_publisher_hosted            ,
    #         sum(case when not is_repository_hosted and is_publisher_hosted then 1 else 0 end) as num_not_repository_hosted_and_has_publisher_hosted
    #         from unpaywall_host_type_derived j
    #         join cdl_journals_temp_with_issn_l_dist_all cdl on j.journal_issn_l = cdl.issn_l
    #         where
    #         j.published_date >= coalesce(cdl.from_date, '1900-01-01'::timestamp) and j.published_date < coalesce(cdl.to_date, '2100-01-01'::timestamp)
    #         group by journal_issn_l
    #     )
    #     (select j.title, j.publisher, j.issns, journal_stats.*
    #                 from journal_stats, ricks_journal j where journal_stats.journal_issn_l = j.issn_l
    #                 )
    #     """

    command = "select * from ricks_unpaywall_journals_subscription_agg;"

    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    return rows

def get_subscriptions():
    responses = []
    rows = get_subscription_rows()
    for row in rows:
        my_dict = {
            "issnl": row["journal_issn_l"],
            "journal_name": row["title"],
            "publisher": row["publisher"],
            "affected_start_date": row["from_date"],
            "affected_end_date": row["to_date"],
            "num_dois": row["num_papers"],
            "num_oa": row["num_is_oa"],
            "proportion_publisher_hosted": float(row["num_publisher_hosted"]) / row["num_papers"],
            "proportion_repository_hosted": float(row["num_repository_hosted"]) / row["num_papers"],
            "proportion_oa": float(row["num_is_oa"]) / row["num_papers"],
            "issns": json.loads(row["issns"]),
            "score": row["num_papers"]
        }
        if my_dict["affected_start_date"]:
            if my_dict["affected_start_date"].isoformat()[0:10].endswith('12-31'):
                my_dict["affected_start_date"] = my_dict["affected_start_date"] + datetime.timedelta(days=1)
            my_dict["affected_start_date"] = my_dict["affected_start_date"].isoformat()[0:10]
        if my_dict["affected_end_date"]:
            my_dict["affected_end_date"] = my_dict["affected_end_date"].isoformat()[0:10]
        responses.append(my_dict)

    responses = sorted(responses, key=lambda k: k['score'], reverse=True)
    return responses


@app.route("/subscriptions.csv", methods=["GET"])
def unpaywall_journals_subscriptions_csv():
    def csv_value(subscription, key):
        if key == "issns":
            return u" " + u";".join(subscription[key]) #need to prefix with space or excel interprets some issns as a date
        if key == "issnl":
            return u" {}".format(subscription[key])  #need to prefix with space or excel interprets some issns as a date
        if "proportion" in key:
            return round(subscription[key], 4)
        return subscription[key]

    subscriptions = get_subscriptions()

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
    responses = get_subscriptions()
    return jsonify({ "list": responses, "count": len(responses)})

@app.route("/subscriptions/name/<q>", methods=["GET"])
def unpaywall_journals_autocomplete_journals(q):
    responses = get_subscriptions()
    filtered_responses = []
    for response in responses:
        if to_unicode_or_bust(q).lower() in to_unicode_or_bust(response["journal_name"]).lower():
            filtered_responses.append(response)
    return jsonify({ "list": filtered_responses, "count": len(filtered_responses)})

@app.route("/subscription/issn/<q>", methods=["GET"])
def unpaywall_journals_issn(q):
    responses = get_subscriptions()
    for response in responses:
        if to_unicode_or_bust(q).lower() in response["issns"]:
            return jsonify(response)
    abort_json(404, u"issn not found in this subscription package")


@app.route("/breakdown", methods=["GET"])
def unpaywall_journals_breakdown():
    rows = get_subscription_rows()
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
            oa_filter = u" and oa_status != 'closed' "
    return oa_filter

def build_text_filter():
    text_filter = ""
    if request.args.get("q", None):
        text_query = request.args.get("q", None)
        if text_query:
            if is_issn(text_query):
                text_filter = u" and u.journal_issn_l = '{}' ".format(text_query)
            elif is_doi(text_query):
                text_filter = u" and u.doi = '{}' ".format(clean_doi(text_query))
            else:
                text_filter = u" and u.title ilike '%{}%' ".format(text_query)
    return text_filter


def get_total_count():

    command = """
            select count(doi) as num_articles
            from unpaywall_production u
            join ricks_unpaywall_journals_subscription_agg j on u.journal_issn_l = j.journal_issn_l
            where 
            u.published_date >= coalesce(j.from_date, '1900-01-01'::timestamp) and u.published_date < coalesce(j.to_date, '2100-01-01'::timestamp)
            {text_filter}
            {oa_filter}
        """.format(text_filter=build_text_filter(),
                   oa_filter=build_oa_filter())

    # print command
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchone()  # just get first row

    return rows["num_articles"]


@app.route("/articles", methods=["GET"])
def unpaywall_journals_articles_paged():

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
        abort_json(400, u"pagesize too large; max 1000")

    offset = (page - 1) * pagesize

    # command = """
    #         select api_json
    #         from unpaywall_production j
    #         join cdl_journals_temp_with_issn_l_dist_all cdl on j.journal_issn_l = cdl.issn_l
    #         where
    #         (j.published_date >= coalesce(cdl.from_date, '1900-01-01'::timestamp) and j.published_date < coalesce(cdl.to_date, '2100-01-01'::timestamp))
    #         {text_filter}
    #         {oa_filter}
    #         order by published_date desc
    #         limit {pagesize}
    #         offset {offset}
    #     """.format(pagesize=pagesize,
    #                offset=offset,
    #                text_filter=build_text_filter(),
    #                oa_filter=build_oa_filter())

    command = """
        select usimple.doi, api_json 
        from unpaywall_simple_sortkey usimple, 
        (   select doi
            from unpaywall_production u
            join ricks_unpaywall_journals_subscription_agg j on u.journal_issn_l = j.journal_issn_l
            where 
                u.published_date >= coalesce(j.from_date, '1900-01-01'::timestamp) and u.published_date < coalesce(j.to_date, '2100-01-01'::timestamp) 
                {text_filter}
                {oa_filter}
            order by published_date desc
            limit {pagesize}
            offset {offset}) as s
        where usimple.doi=s.doi    
    """.format(pagesize=pagesize,
                   offset=offset,
                   text_filter=build_text_filter(),
                   oa_filter=build_oa_filter())
    print command
    with get_db_cursor() as cursor:
        cursor.execute(command)
        rows = cursor.fetchall()
    responses = [json.loads(row["api_json"]) for row in rows]

    return jsonify({"page": page, "list": responses, "total_count": get_total_count()})




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

    keys = rows[0].keys()
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





if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5003))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)

















