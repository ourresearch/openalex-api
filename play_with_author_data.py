from collections import defaultdict
import argparse

from app import get_db_cursor

def get_cited_authors(author_id):
    q = """select normalized_name, affil.author_id, count(*) as n
        from mag_main_paper_references_id ref
        join mag_main_paper_author_affiliations affil on ref.paper_reference_id=affil.paper_id
        join mag_main_authors author on author.author_id=affil.author_id
        where ref.paper_id in 
            (select affil.paper_id
                    from mag_main_paper_author_affiliations affil
                    join mag_main_authors author on author.author_id=affil.author_id
                    and author.author_id = {author_id})
        group by normalized_name, affil.author_id
        """.format(author_id=author_id)

    with get_db_cursor() as cursor:
        cursor.execute(q)
        rows = cursor.fetchall()
        # print(rows)

    normalized_names = [row["normalized_name"] for row in rows if row["author_id"]==author_id]
    return(normalized_names)


def get_author_ids(n=10, min_papers_in_author_clusters=0):
    q = """
            select *
            from mag_main_paper_author_affiliations affil
            join mag_main_authors author on author.author_id=affil.author_id
            join mag_main_papers paper on paper.paper_id=affil.paper_id
            where year = 2021 
            and paper.created_date='2021-05-10'
            and (doc_type is null or doc_type != 'Patent') 
            order by random()
            limit {n}
        """.format(n=n, min_papers_in_author_clusters=min_papers_in_author_clusters)

    with get_db_cursor() as cursor:
        cursor.execute(q)
        rows = cursor.fetchall()
        # print(rows)

    return(rows)

def normalize_name(display_name):
    import re
    import unidecode

    response = display_name
    response = unidecode.unidecode(response)
    response = re.sub(r'[^\w\s]', '', response)
    response = response.lower()
    return response

def get_existing_clusters_with_this_name(normalized_name):
    q = """select * from mag_main_paper_author_affiliations affil
        join mag_main_authors author on author.author_id=affil.author_id
        join mag_main_papers paper on paper.paper_id=affil.paper_id
        where year = 2021 
        and paper.created_date < '2021-05-10'
        and (doc_type is null or doc_type != 'Patent') 
        and normalized_name = '{}'
        """.format(normalized_name)

    with get_db_cursor() as cursor:
        cursor.execute(q)
        rows = cursor.fetchall()
        # print(rows)

    return(rows)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run stuff.")

    print("hi heather")

    author_id_rows = get_author_ids(n=100, min_papers_in_author_clusters=0)

    counts = defaultdict(int)
    paper_ids_no_match = []
    no_matches = []

    for row in author_id_rows:
        normalized_name = normalize_name(row["display_name"])

        hits = get_existing_clusters_with_this_name(normalized_name)
        if not hits:
            print("no_existing_cluster_with_this_name:", normalized_name)
            counts["no_existing_cluster_with_this_name"] += 1
            continue

        normalized_names = get_cited_authors(row["author_id"])
        if normalized_names:
            print("has_self_citation:", normalized_name)
            counts["has_self_citation"] += 1
            continue

        print(" NO MATCH:", row["normalized_name"], row["reference_count"], row["paper_count"], row["original_venue"], row["doi"])
        counts["no_match"] += 1
        no_matches += [(row["normalized_name"], row["paper_id"], row["reference_count"])]
        paper_ids_no_match += [row["paper_id"]]

    print(no_matches)
    print(paper_ids_no_match)

# defaultdict(<class 'int'>, {'has_self_citation': 46, 'no_match': 20, 'no_existing_cluster_with_this_name': 34})
# [3158383182, 3158151237, 3158156027, 3158541645, 3158569951, 3158396683, 3157611051, 3157925296, 3157428202, 3157502777, 3157992221, 3158912095, 3158704966, 3159882961, 3157665638, 3159099629, 3157587797, 3157688124, 3157742262, 3157711983]
