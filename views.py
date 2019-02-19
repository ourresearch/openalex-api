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

    command = """select vid, num_articles, top_journal_name,
            ts_rank_cd(to_tsvector('only_stop_words', top_journal_name), query, 1) AS rank,
            num_articles + 10000 * ts_rank_cd(to_tsvector('only_stop_words', top_journal_name), query, 1) as score
            from unpaywall_vids, to_tsquery('only_stop_words', '{query_for_search}') query
            where to_tsvector('only_stop_words', top_journal_name) @@ query
            order by num_articles + 10000 * ts_rank_cd(to_tsvector('only_stop_words', top_journal_name), query, 1) desc
            limit 10
    """.format(query_for_search=query_for_search)
    res = db.session.connection().execute(sql.text(command))
    rows = res.fetchall()
    for row in rows:
        ret.append({
            "id": row[0],
            "num_articles": row[1],
            "name": row[2],
            "fulltext_rank": row[3],
            "score": row[4]
        })
    return jsonify({"list": ret, "count": len(ret)})

@app.route("/search/journals/title/simple/<q>", methods=["GET"])
def journal_title_search_simple(q):
    ret = []
    command = """select vid, num_articles, top_journal_name
        from unpaywall_vids
        where top_journal_name ilike '%{str}%'
        order by num_articles desc
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

@app.route("/search/journals/title/new/<query>", methods=["GET"])
def journal_title_search_new(query):
    ret = []

    query_statement = sql.text(ur"""
        with s as (SELECT vid, lower(top_journal_name) as lower_title FROM unpaywall_vids WHERE top_journal_name iLIKE :p0)
        select match, count(*) as score from (
            SELECT regexp_matches(lower_title, :p1, 'g') as match FROM s
            union all
            SELECT regexp_matches(lower_title, :p2, 'g') as match FROM s
            union all
            SELECT regexp_matches(lower_title, :p3, 'g') as match FROM s
            union all
            SELECT regexp_matches(lower_title, :p4, 'g') as match FROM s
        ) s_all
        group by match
        order by score desc, length(match::text) asc
        LIMIT 50;""").bindparams(
            p0='%{}%'.format(query),
            p1=ur'({}\w*?\M)'.format(query),
            p2=ur'({}\w*?(?:\s+\w+){{1}})\M'.format(query),
            p3=ur'({}\w*?(?:\s+\w+){{2}})\M'.format(query),
            p4=ur'({}\w*?(?:\s+\w+){{3}})\M'.format(query)
        )

    rows = db.engine.execute(query_statement).fetchall()
    # print rows
    phrases = [{"phrase":row[0][0], "score":row[1]} for row in rows if row[0][0]]
    # print phrases
    ret = phrases
    return jsonify({"list": ret, "count": len(ret)})


@app.route("/search/institutions/name/new/<query>", methods=["GET"])
def institutions_name_search_new(query):
    ret = []

    query_statement = sql.text(ur"""
        with s as (SELECT id as grid_id, lower(org) as lower_org FROM bq_grid_base WHERE org iLIKE :p0)
        select match, count(*) as score from (
            SELECT regexp_matches(lower_org, :p1, 'g') as match FROM s
            union all
            SELECT regexp_matches(lower_org, :p2, 'g') as match FROM s
            union all
            SELECT regexp_matches(lower_org, :p3, 'g') as match FROM s
            union all
            SELECT regexp_matches(lower_org, :p4, 'g') as match FROM s
        ) s_all
        group by match
        order by score desc, length(match::text) asc
        LIMIT 50;""").bindparams(
            p0='%{}%'.format(query),
            p1=ur'({}\w*?\M)'.format(query),
            p2=ur'({}\w*?(?:\s+\w+){{1}})\M'.format(query),
            p3=ur'({}\w*?(?:\s+\w+){{2}})\M'.format(query),
            p4=ur'({}\w*?(?:\s+\w+){{3}})\M'.format(query)
        )

    rows = db.engine.execute(query_statement).fetchall()
    # print rows
    phrases = [{"phrase":row[0][0], "score":row[1]} for row in rows if row[0][0]]
    # print phrases
    ret = phrases
    return jsonify({"list": ret, "count": len(ret)})




if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)

















