from app import db
from institution import Institution

class TransformativeAgreementIssnlMatches(db.Model):
    __tablename__ = 'bq_transformative_agreement_issnl_matches'
    id = db.Column(db.Text, db.ForeignKey("bq_transformative_agreement.id"), primary_key=True)
    issnl = db.Column(db.Text, db.ForeignKey("bq_our_journals_issnl.issnl"), primary_key=True)

    def to_dict(self):
        response = [self.id, self.issnl]
        return response


class TransformativeAgreement(db.Model):
    __tablename__ = 'bq_transformative_agreement'
    id = db.Column(db.Text, primary_key=True)
    publisher_or_journal = db.Column(db.Text)
    publisher_string = db.Column(db.Text)
    issnl = db.Column(db.Text)
    subscriber = db.Column(db.Text)
    country_code = db.Column(db.Text, db.ForeignKey("bq_institutions.country_code"))
    grid_id = db.Column(db.Text, db.ForeignKey("bq_institutions.grid_id"))
    start_date = db.Column(db.Text)
    end_date = db.Column(db.Text)
    notes = db.Column(db.Text)
    link = db.Column(db.Text)

    issnl_matches = db.relationship(
        'TransformativeAgreementIssnlMatches',
        lazy='subquery',
        cascade="all"
    )

    @property
    def issnls_list(self):
        return [obj.issnl for obj in self.issnl_matches]

    @property
    def institutions_list(self):
        if self.grid_id:
            return [self.grid_id]
        if self.country_code:
            institutions = Institution.query.filter(Institution.country_code==self.country_code).all()
            institution_ids = [inst.grid_id for inst in institutions]
            return institution_ids
        return []

    def to_dict(self):
        between_publisher = None
        if self.issnl:
            between_publisher = {"type": "journal", "id": self.issnl}
        elif self.publisher_string:
            between_publisher = {"type": "publisher", "id": self.publisher_string}

        between_institution = None
        if self.grid_id:
            between_institution = {"type": "institution", "id": self.grid_id}
        elif self.country_code:
            between_institution = {"type": "country", "id": self.country_code}

        response = {
            "id": self.id,
            "between": [between_publisher, between_institution],
            "start_date": self.start_date,
            "end_date": self.end_date,
            "notes": self.notes,
            "link": self.link,
            "matches": {
                "issnls": self.issnls_list,
                "institutions": self.institutions_list
            }
        }
        return response