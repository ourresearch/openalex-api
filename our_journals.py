import requests
import re

from app import db
from topic import Topic

class BqOurJournalsIssnl(db.Model):
    __tablename__ = 'bq_our_journals_issnl'
    issnl  =  db.Column(db.Text, primary_key=True)
    title	 =  db.Column(db.Text)
    sjr	 =  db.Column(db.Numeric)
    sjr_best_quartile	 =  db.Column(db.Text)
    h_index	 =  db.Column(db.Numeric)
    country	 =  db.Column(db.Text)
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
    def is_plan_s_compliant(self):
        return self.is_gold_oa

    @property
    def is_gold_oa(self):
        return self.prop_cc_by_since_2018 >= 0.9

    def to_dict_journal_row(self):
        plan_s_policy = {"compliant": False, "reason": []}
        if self.is_gold_oa:
            plan_s_policy = {"compliant": True, "reason": ["gold_oa"]}

        response = {
            "issnl": self.issnl,
            # "url": self.get_journal_url_from_issn(),
            "name": self.title,
            "topics": [t.topic for t in self.topics],
            "publisher": self.publisher,
            "country": self.country,
            "num_articles_since_2018": self.num_articles_since_2018,
            "h_index": self.h_index,
            "policy_compliance": {"plan_s": plan_s_policy}
        }
        return response


    def to_dict_full(self):
        response = self.to_dict_journal_row()
        response["url"] = self.get_journal_url_from_issn()
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

        return response
