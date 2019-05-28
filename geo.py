from app import db

class MyMixin(object):

    @property
    def country_iso2_display(self):
        if hasattr(self, "country_iso2"):
            return self.country_iso2
        else:
            return None

    @property
    def country_iso3_display(self):
        if hasattr(self, "country_iso3"):
            return self.country_iso3
        else:
            return None

    @property
    def continent_display(self):
        if hasattr(self, "continent"):
            return self.continent
        else:
            return None

    @property
    def subcontinent_display(self):
        if hasattr(self, "subcontinent"):
            return self.subcontinent
        else:
            return None

    @property
    def year_int(self):
        return int(self.year)

    @property
    def bronze_green_gold_hybrid(self):
        return self.is_oa

    @property
    def na(self):
        return self.num_distinct_articles

    @property
    def closed(self):
        return self.num_distinct_articles

    def __repr__(self):
        return u"{} ({}, {})".format(self.__class__.__name__, self.lookup, self.year_int)


class OAMonitorUnpaywallByCountry(db.Model, MyMixin):
    __tablename__ = 'oamonitor_unpaywall_by_country'
    __bind_key__ = "redshift_db"

    country = db.Column(db.Text, primary_key=True)
    country_iso2 = db.Column(db.Text)
    country_iso3 = db.Column(db.Text)
    subcontinent = db.Column(db.Text)
    continent = db.Column(db.Text)
    year = db.Column(db.Text, primary_key=True)
    num_distinct_articles = db.Column(db.Numeric)
    is_oa = db.Column(db.Numeric)
    bronze = db.Column(db.Numeric)
    green = db.Column(db.Numeric)
    gold = db.Column(db.Numeric)
    hybrid = db.Column(db.Numeric)
    bronze_green = db.Column(db.Numeric)
    bronze_gold = db.Column(db.Numeric)
    bronze_hybrid = db.Column(db.Numeric)
    green_gold = db.Column(db.Numeric)
    green_hybrid = db.Column(db.Numeric)
    gold_hybrid = db.Column(db.Numeric)
    bronze_green_gold = db.Column(db.Numeric)
    bronze_green_hybrid = db.Column(db.Numeric)
    bronze_gold_hybrid = db.Column(db.Numeric)
    green_gold_hybrid = db.Column(db.Numeric)

    @property
    def lookup(self):
        return self.country

    def to_dict(self):
        return {
            "country": self.country,
            "year": self.year
        }

class OAMonitorUnpaywallBySubcontinent(db.Model, MyMixin):
    __tablename__ = 'oamonitor_unpaywall_by_subcontinent'
    __bind_key__ = "redshift_db"

    subcontinent = db.Column(db.Text, primary_key=True)
    continent = db.Column(db.Text)
    year = db.Column(db.Text, primary_key=True)
    num_distinct_articles = db.Column(db.Numeric)
    is_oa = db.Column(db.Numeric)
    bronze = db.Column(db.Numeric)
    green = db.Column(db.Numeric)
    gold = db.Column(db.Numeric)
    hybrid = db.Column(db.Numeric)
    bronze_green = db.Column(db.Numeric)
    bronze_gold = db.Column(db.Numeric)
    bronze_hybrid = db.Column(db.Numeric)
    green_gold = db.Column(db.Numeric)
    green_hybrid = db.Column(db.Numeric)
    gold_hybrid = db.Column(db.Numeric)
    bronze_green_gold = db.Column(db.Numeric)
    bronze_green_hybrid = db.Column(db.Numeric)
    bronze_gold_hybrid = db.Column(db.Numeric)
    green_gold_hybrid = db.Column(db.Numeric)

    @property
    def lookup(self):
        return self.subcontinent

    def to_dict(self):
        return {
            "subcontinent": self.subcontinent,
            "year": self.year
        }

class OAMonitorUnpaywallByContinent(db.Model, MyMixin):
    __tablename__ = 'oamonitor_unpaywall_by_continent'
    __bind_key__ = "redshift_db"

    continent = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Text, primary_key=True)
    num_distinct_articles = db.Column(db.Numeric)
    is_oa = db.Column(db.Numeric)
    bronze = db.Column(db.Numeric)
    green = db.Column(db.Numeric)
    gold = db.Column(db.Numeric)
    hybrid = db.Column(db.Numeric)
    bronze_green = db.Column(db.Numeric)
    bronze_gold = db.Column(db.Numeric)
    bronze_hybrid = db.Column(db.Numeric)
    green_gold = db.Column(db.Numeric)
    green_hybrid = db.Column(db.Numeric)
    gold_hybrid = db.Column(db.Numeric)
    bronze_green_gold = db.Column(db.Numeric)
    bronze_green_hybrid = db.Column(db.Numeric)
    bronze_gold_hybrid = db.Column(db.Numeric)
    green_gold_hybrid = db.Column(db.Numeric)

    @property
    def lookup(self):
        return self.continent

    def to_dict(self):
        return {
            "continent": self.continent,
            "year": self.year
        }

class OAMonitorUnpaywallWorldwide(db.Model, MyMixin):
    __tablename__ = 'oamonitor_unpaywall_worldwide'
    __bind_key__ = "redshift_db"

    year = db.Column(db.Text, primary_key=True)
    num_distinct_articles = db.Column(db.Numeric)
    is_oa = db.Column(db.Numeric)
    bronze = db.Column(db.Numeric)
    green = db.Column(db.Numeric)
    gold = db.Column(db.Numeric)
    hybrid = db.Column(db.Numeric)
    bronze_green = db.Column(db.Numeric)
    bronze_gold = db.Column(db.Numeric)
    bronze_hybrid = db.Column(db.Numeric)
    green_gold = db.Column(db.Numeric)
    green_hybrid = db.Column(db.Numeric)
    gold_hybrid = db.Column(db.Numeric)
    bronze_green_gold = db.Column(db.Numeric)
    bronze_green_hybrid = db.Column(db.Numeric)
    bronze_gold_hybrid = db.Column(db.Numeric)
    green_gold_hybrid = db.Column(db.Numeric)

    @property
    def lookup(self):
        return "global"

    def to_dict(self):
        return {
            "year": self.year
        }
