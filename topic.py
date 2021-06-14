from app import db

class Topic(db.Model):
    __tablename__ = 'bq_scimago_issnl_topics'
    __bind_key__ = "unpaywall_db"

    issnl = db.Column(db.Text, db.ForeignKey("bq_our_journals_issnl.issnl"), primary_key=True)
    topic = db.Column(db.Text, primary_key=True)
    num_articles_3years = db.Column(db.Numeric(asdecimal=False))
    quadrant = db.Column(db.Numeric(asdecimal=False))

    def to_dict(self):
        response = [self.topic, self.quadrant]
        return response


