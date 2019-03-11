import random
import re
from sqlalchemy import sql

from app import db
from topic import Topic
from data.funders import funder_names
from transformative_agreement import TransformativeAgreement

THRESHOLD_PROP_CC_BY_SINCE_2018 = .90

class Journal(db.Model):
    __tablename__ = 'bq_our_journals_issnl'
    issnl  =  db.Column(db.Text, primary_key=True)
    title	 =  db.Column(db.Text)
    sjr	 =  db.Column(db.Numeric)
    sjr_best_quartile	 =  db.Column(db.Text)
    h_index	 =  db.Column(db.Numeric)
    cites_per_article	 =  db.Column(db.Numeric)
    country	 =  db.Column(db.Text)
    publisher_country_code = db.Column(db.Text)
    publisher_continent = db.Column(db.Text)
    society_or_institution	 =  db.Column(db.Text)
    publisher	 =  db.Column(db.Text)
    categories	 =  db.Column(db.Text)
    num_articles	 =  db.Column(db.Numeric)
    num_cc_by	 =  db.Column(db.Numeric)
    prop_cc_by	 =  db.Column(db.Numeric)
    prop_oa	 =  db.Column(db.Numeric)
    num_oa	 =  db.Column(db.Numeric)
    num_articles_since_2018	 =  db.Column(db.Numeric)
    num_cc_by_since_2018	 =  db.Column(db.Numeric)
    prop_cc_by_since_2018	 =  db.Column(db.Numeric)
    prop_oa_since_2018	 =  db.Column(db.Numeric)
    num_oa_since_2018	 =  db.Column(db.Numeric)
    five_dois	 =  db.Column(db.Text)
    newest_published_date	 =  db.Column(db.Text)
    oldest_published_date	 =  db.Column(db.Text)
    has_apcs	 =  db.Column(db.Text)
    apc_url	 =  db.Column(db.Text)
    apc_fee	 =  db.Column(db.Numeric)
    apc_currency	 =  db.Column(db.Text)
    has_submission_fee	=  db.Column(db.Boolean)
    submission_fee_url	 =  db.Column(db.Text)
    submission_fee	 =  db.Column(db.Numeric)
    submission_fee_currency	 =  db.Column(db.Text)
    has_apc_waiver	=  db.Column(db.Boolean)
    apc_waiver_url	 =  db.Column(db.Text)
    first_year_oa	 =  db.Column(db.Numeric)
    languages	 =  db.Column(db.Text)
    editorial_board_url	 =  db.Column(db.Text)
    review_process	 =  db.Column(db.Text)
    review_process_url	 =  db.Column(db.Text)
    aims_scope_url	 =  db.Column(db.Text)
    instructions_to_authors_url	 =  db.Column(db.Text)
    plagiarism_screening_policy	=  db.Column(db.Boolean)
    plagiarism_screening_url	 =  db.Column(db.Text)
    weeks_submission_to_publication	 =  db.Column(db.Numeric)
    oa_statement_url	 =  db.Column(db.Text)
    license	 =  db.Column(db.Text)
    license_attributes	 =  db.Column(db.Text)
    licence_url	 =  db.Column(db.Text)
    author_holds_copyright_no_restictions	=  db.Column(db.Boolean)
    copyright_url	 =  db.Column(db.Text)
    author_holds_publishing_rights_no_restictions	=  db.Column(db.Boolean)
    publishing_rights_url	=  db.Column(db.Text)

    topics = db.relationship(
        'Topic',
        lazy='subquery',
        cascade="all"
    )

    def get_journal_url_from_issn(self):
        # url = "https://portal.issn.org/resource/ISSN/{}?format=json".format(self.issnl)
        # r = requests.get(url)
        # contents = r.text
        # hits = re.findall('"url" : "(http.*)"', contents)
        # if hits:
        #     return hits[0]
        # hits = re.findall('"url" :\s*\[\s*"(http.*?)"', contents, re.DOTALL | re.MULTILINE)
        # if hits:
        #     return hits[0]

        return None

    @property
    def topic_names(self):
        ordered_topics = sorted(self.topics, key=lambda k: k.quadrant, reverse = False)
        return [t.topic for t in ordered_topics]

    @property
    def is_plan_s_compliant(self):
        return self.is_gold_oa

    @property
    def is_gold_oa(self):
        return self.prop_cc_by_since_2018 >= THRESHOLD_PROP_CC_BY_SINCE_2018

    def get_similar_journals(self):
        # use this to try all topics
        # topic_string = ",".join(["'{}'".format(t) for t in self.topic_names])
        # use this to just match on first topic
        first_topic = self.topic_names[0]
        topic_string = ",".join(["'{}'".format(t) for t in [first_topic]])

        command = """select issnl from bq_our_journals_issnl
                where issnl != '{my_issnl}'
                and issnl in (select issnl from bq_scimago_issnl_topics where topic in ({my_topics}))
                and prop_cc_by_since_2018 >= {thresh}
                order by (abs(sjr - {my_sjr})) asc
                limit 4""".format(my_issnl=self.issnl, my_sjr=self.sjr, my_topics=topic_string, thresh=THRESHOLD_PROP_CC_BY_SINCE_2018)
        res = db.session.connection().execute(sql.text(command))
        rows = res.fetchall()

        issnls = [row[0] for row in rows]
        our_journals = Journal.query.filter(Journal.issnl.in_(issnls)).all()
        our_journals.sort(key=lambda this_object: abs(self.sjr - (this_object.sjr or 0)), reverse=False)

        broad_oa_journal_issns = ["1932-6203", "2041-1723", "2045-2322"] #plos one, nature communications, scientific reports
        if "medicine" in u",".join(["'{}'".format(t.lower()) for t in self.topic_names]):
            broad_oa_journal_issns += ["2167-8359", "2046-1402"]  # peerj, f1000research
        broad_oa_journal_issns = [issn for issn in broad_oa_journal_issns if issn not in issnls]
        broad_oa_journals = Journal.query.filter(Journal.issnl.in_(broad_oa_journal_issns)).all()
        our_journals += random.sample(broad_oa_journals, 5 - len(our_journals))

        return our_journals

    def is_compliant(self, funder=None, institution=None):
        funder_dict = self.get_policy_dict(funder, institution)
        return funder_dict["compliant"]

    def transformative_agreement_applies(self, institution_id, transformative_agreement):
        if not institution_id:
            return False

        covers_institution = False

        # if transformative_agreement["grid_id"] and transformative_agreement["grid_id"] == institution_id:
        #     covers_institution = True
        # elif transformative_agreement["country"]:
        #     if transformative_agreement["country"] == institution_id.country:
        #         covers_institution = True
        #
        # if covers_institution:
        #     if transformative_agreement["issn"] and transformative_agreement["issn"] == self.issnl:
        #         return True
        #     if transformative_agreement["publisher"] and self.publisher.lower() in transformative_agreement["publisher"].lower():
        #         return True

        return False

    def get_policy_dict(self, funder_id=None, institution_id=None):
        if not funder_id or funder_id == "null":
            policy = "unspecified"
        else:
            # default
            policy = "not-supported-yet"
            matching_funders = [f for f in funder_names if str(f["id"])==funder_id]
            if matching_funders:
                funder_dict = matching_funders[0]
                policy = funder_dict["policy"]

        policy_dict = {"policy": policy, "compliant": True, "reason": [], "query": {"funder": funder_id, "institution": institution_id}}

        if policy == "plan-s":
            policy_dict["compliant"] = False

            ##### gold oa
            if self.is_gold_oa:
                policy_dict["compliant"] = True
                policy_dict["reason"] = ["gold-oa"]

            #### mirror journals
            if self.title and self.title.lower().endswith(" x"):
                    policy_dict["compliant"] = False
                    policy_dict["reason"] = ["mirror-journal"]

            #### funder specific policies
            # if NEJM and gates, is compliant
            if funder_id=="100000865" and self.issnl=="0028-4793":
                    policy_dict["compliant"] = True
                    policy_dict["reason"] = ["funder-specific-agreement"]

            #### transformative agreements
            if institution_id and "grid" in institution_id:
                all_transformative_agreements = TransformativeAgreement.query.all()
                for my_ta in all_transformative_agreements:
                    if my_ta.applies(self.issnl, institution_id):
                        policy_dict["compliant"] = True
                        policy_dict["reason"] += ["transformative-agreement"]
                        policy_dict["transformative_agreement_id"] = my_ta.id
            #
            # if institution_id == "grid.4372.2":
            #     if self.publisher and "wiley" in self.publisher.lower():
            #         policy_dict["compliant"] = True
            #         policy_dict["reason"] += ["transformative-agreement"]

            # for transformative_agreement in transformative_agreements:
            #     if self.transformative_agreement_applies(institution_id, transformative_agreement):


        return policy_dict


    def to_dict_journal_row(self, funder=None, institution=None):
        try:
            recent_articles = [u"https://doi.org/{}".format(doi) for doi in re.findall(r'\"(10.*?)\"', self.five_dois)]
        except:
            recent_articles = []
        response = {
            "id": self.issnl,
            "name": self.title,
            "topics": [t.to_dict() for t in self.topics],
            "society_or_institution": self.society_or_institution,
            "publisher": self.publisher,
            "country": self.country,
            "country_code": self.publisher_country_code,
            "continent": self.publisher_continent,
            "num_articles_since_2018": self.num_articles_since_2018,
            "h_index": self.h_index,
            "cites_per_article": self.cites_per_article / 100,  # is actually x100 in db
            "sjr": self.sjr,
            "sjr_best_quartile": self.sjr_best_quartile,
            "recent_articles": recent_articles,
            "newest_published_date": self.newest_published_date,
            "oldest_published_date": self.oldest_published_date,
            "policy_compliance": self.get_policy_dict(funder, institution)
        }
        return response


    def to_dict_full(self, funder=None, institution=None):
        response = self.to_dict_journal_row(funder, institution)
        open_dict = {}
        if self.is_plan_s_compliant:
            open_fields =  """num_cc_by
                prop_cc_by
                prop_oa
                num_oa
                num_articles_since_2018
                num_cc_by_since_2018
                prop_cc_by_since_2018
                prop_oa_since_2018
                num_oa_since_2018
                has_apcs
                apc_url
                apc_fee
                apc_currency
                has_submission_fee
                submission_fee_url
                submission_fee
                submission_fee_currency
                has_apc_waiver
                apc_waiver_url
                first_year_oa
                languages
                editorial_board_url
                review_process
                review_process_url
                aims_scope_url
                instructions_to_authors_url
                plagiarism_screening_policy
                plagiarism_screening_url
                weeks_submission_to_publication
                oa_statement_url
                license
                license_attributes
                licence_url
                author_holds_copyright_no_restictions
                copyright_url
                author_holds_publishing_rights_no_restictions
                publishing_rights_url""".split()
            for field in open_fields:
                clean_field = field.strip()
                open_dict[clean_field] = getattr(self, clean_field)
        response["oa_details"] = open_dict
        response["similar_journals"] = [j.to_dict_journal_row(funder, institution) for j in self.get_similar_journals()]

        return response
