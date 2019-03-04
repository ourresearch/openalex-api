from app import db

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


    def to_dict(self):
        response = {
            "id": self.issnl,
            "issnl": self.issnl,
            "name": self.title,
            "policy": {"compliant": False}
        }
        return response