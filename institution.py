import requests
import re
from sqlalchemy import sql

from app import db
from topic import Topic
from data.funders import funder_names
from data.transformative_agreements import transformative_agreements

THRESHOLD_PROP_CC_BY_SINCE_2018 = .90

class BqOurJournalsIssnl(db.Model):
    __tablename__ = 'bq_our_journals_issnl'
    issnl  =  db.Column(db.Text, primary_key=True)
    title	 =  db.Column(db.Text)
    sjr	 =  db.Column(db.Numeric)
    sjr_best_quartile	 =  db.Column(db.Text)
    h_index	 =  db.Column(db.Numeric)
    country	 =  db.Column(db.Text)

