from app import db

class OAMonitorUnpaywallByCountry(db.Model):
    __tablename__ = 'oamonitor_unpaywall_by_country'
    __bind_key__ = "redshift_db"

    country = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Text, primary_key=True)

    def to_dict(self):
        return {
            "country": self.country,
            "year": self.year
        }