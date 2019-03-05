import requests
import re
from app import db

class Topic(db.Model):
    __tablename__ = 'bq_scimago_issnl_topics'
    issnl = db.Column(db.Text, db.ForeignKey("bq_our_journals_issnl.issnl"), primary_key=True)
    topic = db.Column(db.Text, primary_key=True)
    num_articles_3years = db.Column(db.Numeric)
    quadrant = db.Column(db.Numeric)

    def to_dict(self):
        response = [self.topic, self.quadrant]
        return response