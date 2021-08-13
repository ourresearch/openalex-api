import datetime
import requests
from time import time
import urllib.parse
import argparse
import json
import copy

from app import db
from app import logger
from app import get_db_cursor
from util import elapsed
from util import clean_doi



# data from https://archive.org/details/crossref_doi_metadata
# To update the dump, use the public API with deep paging:
# https://api.crossref.org/works?filter=from-update-date:2016-04-01&rows=1000&cursor=*
# The documentation for this feature is available at:
# https://github.com/CrossRef/rest-api-doc/blob/master/rest_api.md#deep-paging-with-cursors


def is_good_file(filename):
    return "chunk_" in filename

def get_api_for_one_doi(doi):
    # needs a mailto, see https://github.com/CrossRef/rest-api-doc#good-manners--more-reliable-service
    headers={"Accept": "application/json", "User-Agent": "mailto:team@impactstory.org"}
    root_url_doi = "https://api.crossref.org/works?filter=doi:{doi}"
    url = root_url_doi.format(doi=doi)
    resp = requests.get(url, headers=headers)
    if resp and resp.status_code == 200:
        resp_data = resp.json()["message"]
        if resp_data["items"]:
            return resp_data["items"][0]
    return None

def add_pubs_from_dois(dois):
    new_pubs = []
    for doi in dois:
        crossref_api = get_api_for_one_doi(doi)
        new_pub = build_new_pub(doi, crossref_api)

        # hack so it gets updated soon
        new_pub.updated = datetime.datetime(1042, 1, 1)

        new_pubs.append(new_pub)

    added_pubs = add_new_pubs(new_pubs)
    return added_pubs


def add_new_pubs_from_dois(dois):
    if not dois:
        return []

    rows = db.session.query(Pub.id).filter(Pub.id.in_(dois)).all()
    dois_in_db = [row[0] for row in rows]
    dois_not_in_db = [doi for doi in dois if doi not in dois_in_db]
    added_pubs = add_pubs_from_dois(dois_not_in_db)
    return added_pubs


def get_new_dois_and_data_from_crossref(query_doi=None, first=None, last=None, today=False, week=False, offset_days=0, chunk_size=1000):
    # needs a mailto, see https://github.com/CrossRef/rest-api-doc#good-manners--more-reliable-service
    headers={"Accept": "application/json", "User-Agent": "mailto:team@impactstory.org"}

    root_url_with_last = "https://api.crossref.org/works?order=desc&sort=updated&filter=from-created-date:{first},until-created-date:{last}&rows={chunk}&cursor={next_cursor}"
    root_url_no_last = "https://api.crossref.org/works?order=desc&sort=updated&filter=from-created-date:{first}&rows={chunk}&cursor={next_cursor}"
    root_url_doi = "https://api.crossref.org/works?filter=doi:{doi}"

    # but if want all changes, use "indexed" not "created" as per https://github.com/CrossRef/rest-api-doc/blob/master/rest_api.md#notes-on-incremental-metadata-updates
    # root_url_with_last = "https://api.crossref.org/works?order=desc&sort=updated&filter=from-indexed-date:{first},until-indexed-date:{last}&rows={chunk}&cursor={next_cursor}"
    # root_url_no_last = "https://api.crossref.org/works?order=desc&sort=updated&filter=from-indexed-date:{first}&rows={chunk}&cursor={next_cursor}"

    next_cursor = "*"
    has_more_responses = True
    num_pubs_added_so_far = 0
    pubs_this_chunk = []

    if week:
        last = (datetime.date.today() + datetime.timedelta(days=1))
        first = (datetime.date.today() - datetime.timedelta(days=7))
    elif today:
        last = (datetime.date.today() + datetime.timedelta(days=1))
        first = (datetime.date.today() - datetime.timedelta(days=2))

    if not first:
        first = datetime.date(2016, 4, 1)

    last = last and last - datetime.timedelta(days=offset_days)
    first = first and first - datetime.timedelta(days=offset_days)

    start_time = time()

    while has_more_responses:

        if query_doi:
            url = root_url_doi.format(doi=query_doi)
        else:
            if last:
                url = root_url_with_last.format(first=first.isoformat(),
                                                last=last.isoformat(),
                                                next_cursor=next_cursor,
                                                chunk=chunk_size)
            else:
                # query is much faster if don't have a last specified, even if it is far in the future
                url = root_url_no_last.format(first=first.isoformat(),
                                              next_cursor=next_cursor,
                                              chunk=chunk_size)

        logger.info(u"calling url: {}".format(url))
        crossref_time = time()

        resp = requests.get(url, headers=headers)
        logger.info(u"getting crossref response took {} seconds".format(elapsed(crossref_time, 2)))
        if resp.status_code != 200:
            logger.info(u"error in crossref call, status_code = {}".format(resp.status_code))
            resp = None

        if resp:
            resp_data = resp.json()["message"]
            next_cursor = resp_data.get("next-cursor", None)
            if next_cursor:
                next_cursor = urllib.parse.quote(next_cursor)

            if not resp_data["items"] or not next_cursor:
                has_more_responses = False

            for api_raw in resp_data["items"]:
                loop_time = time()

                doi = clean_doi(api_raw["DOI"])
                my_pub = build_new_pub(doi, api_raw)

                # hack so it gets updated soon
                my_pub.updated = datetime.datetime(1042, 1, 1)

                pubs_this_chunk.append(my_pub)

                if len(pubs_this_chunk) >= 100:
                    added_pubs = add_new_pubs(pubs_this_chunk)
                    logger.info(u"added {} pubs, loop done in {} seconds".format(len(added_pubs), elapsed(loop_time, 2)))
                    num_pubs_added_so_far += len(added_pubs)

                    # if new_pubs:
                    #     id_links = ["http://api.oadoi.org/v2/{}".format(my_pub.id) for my_pub in new_pubs[0:5]]
                    #     logger.info(u"last few ids were {}".format(id_links))

                    pubs_this_chunk = []

        logger.info(u"at bottom of loop")

    # make sure to get the last ones
    logger.info(u"saving last ones")
    added_pubs = add_new_pubs(pubs_this_chunk)
    num_pubs_added_so_far += len(added_pubs)
    logger.info(u"Added >>{}<< new crossref dois on {}, took {} seconds".format(
        num_pubs_added_so_far, datetime.datetime.now().isoformat()[0:10], elapsed(start_time, 2)))


# this one is used for catch up.  use the above function when we want all weekly dois
def scroll_through_all_dois(query_doi=None, first=None, last=None, today=False, week=False, chunk_size=1000):
    # needs a mailto, see https://github.com/CrossRef/rest-api-doc#good-manners--more-reliable-service
    headers={"Accept": "application/json", "User-Agent": "mailto:team@impactstory.org"}

    # # this is by pub-date instead of created date, for 2017, and includes journal=article filter
    # base_url = "https://api.crossref.org/works?filter=type:journal-article,from-pub-date:2017,until-pub-date:2017&rows=1000&select=DOI&cursor={next_cursor}"

    # base_url = "https://api.crossref.org/works?filter=type:journal-article,from-issued-date:2018,until-issued-date:2018&rows={rows}&select=DOI,published-print,published-online,issued&cursor={next_cursor}"
    # base_url = "https://api.crossref.org/works?filter=type:journal-article,from-issued-date:2018,until-issued-date:2018&rows={rows}&select=DOI,issued&cursor={next_cursor}"

    # if first:
    #     base_url = "https://api.crossref.org/works?filter=from-created-date:{first},until-created-date:{last}&rows={rows}&select=DOI&cursor={next_cursor}"
    # else:
    #     base_url = "https://api.crossref.org/works?filter=until-created-date:{last}&rows={rows}&select=DOI&cursor={next_cursor}"

    if first:
        base_url = "https://api.crossref.org/works?filter=from-created-date:{first},until-created-date:{last}&rows={rows}&cursor={next_cursor}"
    else:
        base_url = "https://api.crossref.org/works?filter=until-created-date:{last}&rows={rows}&cursor={next_cursor}"

    next_cursor = "*"
    has_more_responses = True
    number_added = 0

    while has_more_responses:
        has_more_responses = False

        start_time = time()
        url = base_url.format(
            first=first,
            last=last,
            rows=chunk_size,
            next_cursor=next_cursor)
        logger.info(u"calling url: {}".format(url))

        resp = requests.get(url, headers=headers)
        logger.info(u"getting crossref response took {} seconds.  url: {}".format(elapsed(start_time, 2), url))
        if resp.status_code != 200:
            logger.info(u"error in crossref call, status_code = {}".format(resp.status_code))
            return

        resp_data = resp.json()["message"]
        next_cursor = resp_data.get("next-cursor", None)
        if next_cursor:
            next_cursor = urllib.parse.quote(next_cursor)
            if resp_data["items"] and len(resp_data["items"]) == chunk_size:
                has_more_responses = True

        def get_fields(row_original):
            row = copy.deepcopy(row_original)
            fields = {}
            fields["doi"] = clean_doi(row["DOI"])
            if "reference" in row:
                del row["reference"]
            fields["api_response"] = json.dumps(row)
            fields["api_response"] = fields["api_response"].replace("'", "''")
            if len(fields["api_response"]) > 64000:
                print("remaining api_response still too long even after deleting references")
                print(len(fields["api_response"]))
                print(fields["doi"])
            # fields["dates_text"] = json.dumps(row)  # too slow for now
            fields["dates_text"] = None
            issued_year = ""
            issued_month = ""
            issued_day = ""
            issued = row.get("issued", None)

            if issued:
                issued_date_list = issued.get("date-parts")[0]
            if issued_date_list:
                issued_year = issued_date_list[0]
                try:
                    issued_month = issued_date_list[1]
                except IndexError:
                    pass
                try:
                    issued_day = issued_date_list[2]
                except IndexError:
                    pass
            fields["issued_year"] = issued_year
            fields["issued_month"] = issued_month
            fields["issued_day"] = issued_day
            return fields

        def get_references(items):
            references = []
            for item in items:
                doi = clean_doi(item["DOI"])
                for reference_row in item.get("reference", []):
                    reference = {}
                    reference["doi"] = doi
                    try:
                        reference["doi_referenced"] = clean_doi(reference_row["DOI"])
                    except:
                        reference["doi_referenced"] = None
                    reference["api_response"] = json.dumps(reference_row)
                    reference["api_response"] = reference["api_response"].replace("'", "''")
                    references += [reference]
            return references

        data_dicts = [get_fields(api_raw) for api_raw in resp_data["items"]]
        data_reference_dicts = get_references(resp_data["items"])
        with get_db_cursor() as cursor:
            command = u"""INSERT INTO crossref_raw_direct (doi, updated, api_raw) values """
            insert_strings = []
            for my_dict in data_dicts:
                insert_string = u"""('{doi}', sysdate, '{api_response}')""".format(**my_dict)
                insert_strings.append(insert_string)
            command = command + u",".join(insert_strings) + u";"
            # print command
            print("*"),
            cursor.execute(command)

            if data_reference_dicts:
                command = u"""INSERT INTO crossref_reference_raw_direct (doi, doi_referenced, updated, api_raw) values """
                insert_strings = []
                for my_dict in data_reference_dicts:
                    insert_string = u"""('{doi}', '{doi_referenced}', sysdate, '{api_response}')""".format(**my_dict)
                    insert_strings.append(insert_string)
                command = command + u",".join(insert_strings) + u";"
                # print command
                print("!"),
                cursor.execute(command)

        logger.info(u"loop done in {} seconds".format(elapsed(start_time, 2)))

    return number_added


def date_str(s):
    return datetime.datetime.strptime(s, '%Y-%m-%d').date()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")


    scroll_through_all_dois(None, "2021-05-01", "2021-05-10")

    #
    # function = scroll_through_all_dois
    #
    # parser.add_argument('--first', nargs="?", type=date_str, help="first filename to process (example: --first 2006-01-01)")
    # parser.add_argument('--last', nargs="?", type=date_str, help="last filename to process (example: --last 2006-01-01)")
    #
    # parser.add_argument('--query_doi', nargs="?", type=str, help="pull in one doi")
    #
    # parser.add_argument('--today', action="store_true", default=False, help="use if you want to pull in crossref records from last 2 days")
    # parser.add_argument('--week', action="store_true", default=False, help="use if you want to pull in crossref records from last 7 days")
    #
    # parser.add_argument('--chunk_size', nargs="?", type=int, default=1000, help="how many docs to put in each POST request")
    # parser.add_argument('--offset_days', nargs="?", type=int, default=0, help="advance the import date range by this many days")
    #
    # parsed = parser.parse_args()
    #
    # logger.info(u"calling {} with these args: {}".format(function.__name__, vars(parsed)))
    # function(**vars(parsed))
    #
