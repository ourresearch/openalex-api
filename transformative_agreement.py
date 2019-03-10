from sqlalchemy import orm
from app import db
from institution import Institution

class TransformativeAgreementIssnlMatches(db.Model):
    __tablename__ = 'bq_transformative_agreement_issnl_matches'
    id = db.Column(db.Text, db.ForeignKey("bq_transformative_agreement.id"), primary_key=True)
    issnl = db.Column(db.Text, primary_key=True)

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
    def journals_list(self):
        from journal import Journal

        issnls =  [obj.issnl for obj in self.issnl_matches]
        journals = Journal.query.filter(Journal.issnl.in_(issnls)).options(orm.noload("*")).all()
        journal_dicts = [{"id": j.issnl, "name": j.title} for j in journals]
        return journal_dicts


    @property
    def institutions_list(self):
        institutions = []
        if self.grid_id:
            institutions = Institution.query.filter(Institution.grid_id==self.grid_id).options(orm.noload("*")).all()
        elif self.country_code:
            institutions = Institution.query.filter(Institution.country_code==self.country_code).options(orm.noload("*")).all()
        institution_dicts = [{"id": inst.grid_id, "name": inst.org_name} for inst in institutions]
        return institution_dicts

    def applies(self, issnl, grid_id):
        if (not self.issnl_matches and not self.issnl) or not self.institutions_list:
            return False
        if issnl not in [match.issnl for match in self.issnl_matches] + [self.issnl]:
            return False
        if grid_id not in [inst["id"] for inst in self.institutions_list]:
            return False
        return True

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
                "journals": self.journals_list,
                "institutions": self.institutions_list
            }
        }
        return response