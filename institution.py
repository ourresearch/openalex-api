
from app import db


class Institution(db.Model):
    __tablename__ = 'bq_institutions'
    grid_id = db.Column(db.Text, primary_key=True)
    org_name = db.Column(db.Text)
    country = db.Column(db.Text)
    country_code = db.Column(db.Text)
    continent = db.Column(db.Text)
    num_papers = db.Column(db.Numeric)

    def to_dict(self):
        response = {
            "id": self.grid_id,
            "name": self.org_name,
            "country": self.country,
            "country_code": self.country_code,
            "continent": self.continent,
            "num_papers": self.num_papers
        }
        return response
