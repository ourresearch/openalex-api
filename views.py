from flask import make_response
from flask import request
from flask import redirect
from flask import abort
from flask import render_template
from flask import jsonify
from flask import g

import json
import os
import sys
import re
from time import time


from app import app
from app import db
from app import logger

from sqlalchemy import sql

from data.funders import funder_names
from our_journals import BqOurJournalsIssnl
from topic import Topic



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

    # remove session
    db.session.remove()

    # without this jason's heroku local buffers forever
    sys.stdout.flush()

    return resp



@app.before_request
def stuff_before_request():

    g.request_start_time = time()

    # don't redirect http api in some cases
    if request.url.startswith("http://api."):
        return
    if "staging" in request.url or "localhost" in request.url:
        return

    # redirect everything else to https.
    new_url = None
    try:
        if request.headers["X-Forwarded-Proto"] == "https":
            pass
        elif "http://" in request.url:
            new_url = request.url.replace("http://", "https://")
    except KeyError:
        # logger.info(u"There's no X-Forwarded-Proto header; assuming localhost, serving http.")
        pass

    if new_url:
        return redirect(new_url, 301)  # permanent


@app.route("/test", methods=["GET"])
def get_example():
    return jsonify({"results": "hi"})

@app.route('/', methods=["GET", "POST"])
def base_endpoint():
    return jsonify({
        "version": "0.0.1",
        "msg": "Don't panic"
    })


@app.route("/search/journals/title/<q>", methods=["GET"])
def journal_title_search(q):
    ret = []

    query_for_search = re.sub(r'[!\'()|&]', ' ', q).strip()
    if query_for_search:
        query_for_search = re.sub(r'\s+', ' & ', query_for_search)
        query_for_search += ':*'

    command = """select 
                vid, 
                num_articles_since_2018, 
                top_journal_name, 
                prop_cc_by_since_2018,
                ts_rank_cd(to_tsvector('only_stop_words', top_journal_name), query, 1) AS rank,
                num_articles + 10000 * ts_rank_cd(to_tsvector('only_stop_words', top_journal_name), query, 1) as score
            
            from bq_our_journals, to_tsquery('only_stop_words', '{query_for_search}') query
            where to_tsvector('only_stop_words', top_journal_name) @@ query
            order by num_articles_since_2018 + 10000 * ts_rank_cd(to_tsvector('only_stop_words', top_journal_name), query, 1) desc
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



@app.route("/search/institutions/name/<q>", methods=["GET"])
def institutions_name_search(q):
    ret = []
    command = """select grid_id, num_papers, org_name
        from bq_org_name_by_num_papers
        where org_name ilike '%{str}%'
        order by num_papers desc
        limit 10
    """.format(str=q)
    res = db.session.connection().execute(sql.text(command))
    rows = res.fetchall()
    for row in rows:
        ret.append({
            "id": row[0],
            "num_articles": row[1],
            "name": row[2]
        })
    return jsonify({"list": ret, "count": len(ret)})


@app.route("/search/funders/name/<q>", methods=["GET"])
def funders_name_search(q):

    ret = [funder for funder in funder_names if q.lower() in funder["alternate_names"].lower()]

    return jsonify({"list": ret, "count": len(ret)})


@app.route("/journal/<issnl_query>", methods=["GET"])
def journal_issnl_get(issnl_query):
    my_journal = BqOurJournalsIssnl.query.filter(BqOurJournalsIssnl.issnl == issnl_query).first()
    return jsonify(my_journal.to_dict_full())


@app.route("/topic/<topic_query>", methods=["GET"])
def topic_get(topic_query):
    topic_hits = Topic.query.filter(Topic.topic == topic_query).order_by(Topic.num_articles_3years.desc()).limit(50)
    our_journals = BqOurJournalsIssnl.query.filter(BqOurJournalsIssnl.issnl.in_([t.issnl for t in topic_hits])).all()
    responses = [j.to_dict_journal_row() for j in our_journals]
    responses = sorted(responses, key=lambda k: k['num_articles_since_2018'], reverse=True)
    return jsonify({ "list": responses, "count": len(responses)})



@app.route("/search/journals", methods=["GET"])
def search_journals_get():

    journal = request.args.get("q", None)
    funder = request.args.get("funder", None)
    institution = request.args.get("institution", None)

    if not journal:
        abort_json(422, "missing journal")

    response = []

    query_for_search = re.sub(r'[!\'()|&]', ' ', journal).strip()
    if query_for_search:
        query_for_search = re.sub(r'\s+', ' & ', query_for_search)
        query_for_search += ':*'

    command = """select 
                issnl, 
                num_articles_since_2018, 
                title, 
                prop_cc_by_since_2018,
                ts_rank_cd(to_tsvector('only_stop_words', title), query, 1) AS rank,
                num_articles + 10000 * ts_rank_cd(to_tsvector('only_stop_words', title), query, 1) as score
            from bq_our_journals_issnl, to_tsquery('only_stop_words', '{query_for_search}') query
            where to_tsvector('only_stop_words', title) @@ query
            order by num_articles_since_2018 + 10000 * ts_rank_cd(to_tsvector('only_stop_words', title), query, 1) desc
            limit 20
    """.format(query_for_search=query_for_search)
    res = db.session.connection().execute(sql.text(command))
    rows = res.fetchall()

    issnls = [row[0] for row in rows]
    our_journals = BqOurJournalsIssnl.query.filter(BqOurJournalsIssnl.issnl.in_(issnls)).all()
    # print our_journals
    responses = []
    for this_journal in our_journals:
        response = this_journal.to_dict_journal_row()
        matching_score_row = [row for row in rows if row[0]==this_journal.issnl][0]
        response["fulltext_rank"] = matching_score_row[4]
        response["score"] = matching_score_row[5]
        responses.append(response)

    # if "Water Research" in journal:
    #     response.append(
    #     {
    #       "fulltext_rank": 0.0910239,
    #       "id": "13846",
    #       "metrics": {
    #         "num_articles_since_2018": 42
    #       },
    #       "name": "Water Research X",
    #       "plan_s_policy": {
    #             "compliant": False,
    #             "details": ["mirror_journal"]
    #         },
    #       "score": 21145.2392196655
    #     }
    #     )


    results = sorted(responses, key=lambda k: k['score'], reverse=True)

    return jsonify({ "list": responses, "count": len(responses)})



if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)

















