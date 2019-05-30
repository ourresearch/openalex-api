from flask import request
from time import time
from util import elapsed
from collections import defaultdict
from sqlalchemy.orm import deferred
from sqlalchemy.orm import undefer
from sqlalchemy.orm import synonym
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import column_property
from cached_property import cached_property

from app import db
from app import get_db_cursor


# really speeds things up to preload these, need them as a denominator for everything
global_objects = None

def get_oa_column(oa_filter_list):

    all_columns = OAMonitorUnpaywallByCountry.__table__.columns
    for attr in all_columns:
        attr_name = attr.name
        attr_name_parts = sorted([w.lower() for w in attr_name.split("_")])
        if set(oa_filter_list) == set(attr_name_parts):
            return attr_name
    return u"_".join(sorted(oa_filter_list))

def get_geo_rows(groupby, oa_filter_list):
    undefer_column = get_oa_column(oa_filter_list)
    if groupby == "country":
        objects = OAMonitorUnpaywallByCountry.query.options(undefer(undefer_column)).all()
    elif groupby == "subcontinent":
        objects = OAMonitorUnpaywallBySubcontinent.query.options(undefer(undefer_column)).all()
    elif groupby == "continent":
        objects = OAMonitorUnpaywallByContinent.query.options(undefer(undefer_column)).all()
    else:
        objects = global_objects
    return objects

def get_oa_column_name(oa_filter_list):
    all_columns = "bronze_gold_green_hybrid".split("_")
    if set(oa_filter_list) == set(all_columns):
        return "is_oa"
    valid_columns = """
    bronze
    green
    gold
    hybrid
    bronze_green
    bronze_gold
    bronze_hybrid
    green_gold
    green_hybrid
    gold_hybrid
    bronze_green_gold
    bronze_green_hybrid
    bronze_gold_hybrid
    green_gold_hybrid""".split()
    for attr_raw in valid_columns:
        attr_name = attr_raw.strip()
        attr_name_parts = sorted([w.lower() for w in attr_name.split("_")])
        if set(oa_filter_list) == set(attr_name_parts):
            return attr_name
    return u"_".join(sorted(oa_filter_list))

lookup = {
    "country": {
        "__tablename__": 'oamonitor_unpaywall_by_country',
        "__columns__": 'country, country_iso2, country_iso3, subcontinent, continent, year, num_distinct_articles, is_oa'
    },
    "subcontinent": {
        "__tablename__": 'oamonitor_unpaywall_by_subcontinent',
        "__columns__": 'subcontinent, continent, year, num_distinct_articles, is_oa'
    },
    "continent": {
        "__tablename__": 'oamonitor_unpaywall_by_continent',
        "__columns__" : 'continent, year, num_distinct_articles, is_oa'
    },
    "global": {
        "__tablename__": 'oamonitor_unpaywall_worldwide',
        "__columns__" : 'year, num_distinct_articles, is_oa'
    },
}

def get_all_rows_fast(groupby, oa_column):
    timing = {}
    start_time = time()
    with get_db_cursor() as cursor:
        timing["0. in with"] = elapsed(start_time)

        start_time = time()
        q = "select {}, {} from {} where year='2018'".format(lookup[groupby]["__columns__"],
                                             oa_column,
                                             lookup[groupby]["__tablename__"])
        # print q
        cursor.execute(q)
        timing["1. after execute"] = elapsed(start_time)

        start_time = time()
        rows = cursor.fetchall()
        timing["2. after fetchall"] = elapsed(start_time)
    return (rows, timing)

def objects_from_rows(groupby, rows):
    class_name = u"Geo{}".format(groupby.title())
    my_class = globals()[class_name]
    my_objects = [my_class(row) for row in rows]
    return my_objects

class GeoObject(object):

    def __init__(self, row):
        self._row = row
        for (k, v) in row.iteritems():
            setattr(self, k, v)

    @classmethod
    def get_all_rows(cls, oa_column):
        with get_db_cursor() as cursor:
            q = "select {}, {} from {}".format(cls.__columns__,
                                                         oa_column,
                                                         cls.__tablename__)
            # print q
            cursor.execute(q)
            rows = cursor.fetchall()
        my_objects = [cls(row) for row in rows]
        return my_objects


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
    def bronze_gold_green_hybrid(self):
        return self.is_oa

    @property
    def na(self):
        return self.num_distinct_articles

    def __repr__(self):
        return u"{} ({}, {})".format(self.__class__.__name__, self.lookup, self.year_int)



class GeoCountry(GeoObject):
    __tablename__ = 'oamonitor_unpaywall_by_country'
    __columns__ = 'country, country_iso2, country_iso3, subcontinent, continent, year, num_distinct_articles, is_oa'

    @property
    def lookup(self):
        return self.country


class GeoSubcontinent(GeoObject):
    __tablename__ = 'oamonitor_unpaywall_by_subcontinent'
    __columns__ = 'subcontinent, continent, year, num_distinct_articles, is_oa'

    @property
    def lookup(self):
        return self.subcontinent


class GeoContinent(GeoObject):
    __tablename__ = 'oamonitor_unpaywall_by_continent'
    __columns__ = 'continent, year, num_distinct_articles, is_oa'

    @property
    def lookup(self):
        return self.continent


class GeoGlobal(GeoObject):
    __tablename__ = 'oamonitor_unpaywall_worldwide'
    __columns__ = 'year, num_distinct_articles, is_oa'

    @property
    def lookup(self):
        return "global"



def get_oa_from_redshift(my_key):
    timing = {}
    start_time = time()

    since_year = int(request.args.get("since", "2009"))

    oa_request = request.args.get("oa", "na")
    if oa_request in ("all", "any"):
        oa_request = "bronze,green,gold,hybrid"
    oa_filter_list = [w.strip() for w in oa_request.lower().split(",")]
    timing["0. prep_elapsed"] = elapsed(start_time)

    global_response = None
    if my_key and my_key != "global":
        (global_response, global_timing) = get_oa_from_redshift("global")
        timing["0.5. get_global"] = global_timing
    else:
        my_key = "global"

    this_start = time()
    objects = get_geo_rows(my_key, oa_filter_list)
    timing["1. get_geo_rows"] = elapsed(this_start)
    this_start = time()

    response = {}
    out_of_over_years = defaultdict(int)
    value_over_years = defaultdict(int)
    oa_histogram = defaultdict(list)
    oa_data_column = get_oa_column(oa_filter_list)

    for obj in objects:
        if obj.year_int >= since_year and obj.year_int < 2019:
            column_value = getattr(obj, oa_data_column)
            oa_histogram[obj.lookup] += [(obj.year_int,
                                      round(float(column_value)/int(obj.num_distinct_articles), 5))]
            value_over_years[obj.lookup] += int(column_value)
            out_of_over_years[obj.lookup] += int(obj.num_distinct_articles)

    timing["2. first_loop"] = elapsed(this_start)
    this_start = time()

    for obj in objects:
        if since_year==obj.year_int and obj.lookup:
            distinct_articles_proportion_global = 1
            if global_response:
                distinct_articles_proportion_global = float(out_of_over_years[obj.lookup]) / global_response["global"]["articles"]["num_total"]
            sorted_histogram = sorted(oa_histogram[obj.lookup], key=lambda x: x[0], reverse=False)
            if out_of_over_years[obj.lookup]:
                prop_oa = round(float(value_over_years[obj.lookup])/out_of_over_years[obj.lookup], 5)
            else:
                prop_oa = None
            my_dict = {
                "name": obj.lookup,
                "name_iso2": obj.country_iso2_display,
                "name_iso3": obj.country_iso3_display,
                "continent": obj.continent_display,
                "subcontinent": obj.subcontinent_display,
                "since": obj.year_int,
                "oa_types": oa_filter_list,
                "articles": {
                    "num_total": out_of_over_years[obj.lookup],
                    "prop_global": round(distinct_articles_proportion_global, 5),
                    "num_oa": value_over_years[obj.lookup],
                    "prop_oa": prop_oa,
                    "prop_oa_by_year": sorted_histogram
                }
            }
            response[obj.lookup] = my_dict

    timing["3. second_loop"] = elapsed(this_start)

    return (response, timing)



def get_oa_from_redshift_fast(groupby):
    timing = {}
    start_time = time()

    since_year = request.args.get("since", "2009")

    oa_request = request.args.get("oa", "na")
    if oa_request in ("all", "any"):
        oa_request = "bronze,green,gold,hybrid"
    oa_filter_list = [w.strip() for w in oa_request.lower().split(",")]
    timing["0. prep_elapsed"] = elapsed(start_time)

    global_response = None
    if groupby and groupby != "global":
        (global_response, global_timing) = get_oa_from_redshift("global")
        timing["0.5. get_global"] = global_timing
    else:
        groupby = "global"

    this_start = time()
    undefer_column = get_oa_column_name(oa_filter_list)
    (rows, rows_timing) = get_all_rows_fast(groupby, undefer_column)
    timing["1. get_geo_rows"] = rows_timing
    this_start = time()

    objects = objects_from_rows(groupby, rows)
    timing["1.5 get_geo_objects"] = elapsed(this_start)
    this_start = time()

    response = {}
    oa_histogram = defaultdict(list)
    oa_data_column = get_oa_column(oa_filter_list)

    for obj in objects:
        if obj.year >= since_year and obj.year_int < 2019:
            column_value = getattr(obj, oa_data_column)
            oa_histogram[obj.lookup] += [(obj.year_int,
                                      round(float(column_value)/int(obj.num_distinct_articles), 5))]

    timing["2. first_loop"] = elapsed(this_start)

    this_start = time()

    sorted_objects = sorted(objects, key=lambda x: x.lookup, reverse=False)

    for obj in objects:
        if since_year==obj.year and obj.lookup:
            column_value = getattr(obj, oa_data_column)
            distinct_articles_proportion_global = 1
            if global_response:
                distinct_articles_proportion_global = float(obj.num_distinct_articles) / global_response["global"]["articles"]["num_total"]
            sorted_histogram = sorted(oa_histogram[obj.lookup], key=lambda x: x[0], reverse=False)
            if obj.num_distinct_articles:
                prop_oa = round(float(column_value)/obj.num_distinct_articles, 5)
            else:
                prop_oa = None
            my_dict = {
                "name": obj.lookup,
                "name_iso2": obj.country_iso2_display,
                "name_iso3": obj.country_iso3_display,
                "continent": obj.continent_display,
                "subcontinent": obj.subcontinent_display,
                "since": obj.year_int,
                "oa_types": oa_filter_list,
                "articles": {
                    "num_total": obj.num_distinct_articles,
                    "prop_global": round(distinct_articles_proportion_global, 5),
                    "num_oa": column_value,
                    "prop_oa": prop_oa,
                    "prop_oa_by_year": sorted_histogram
                }
            }
            response[obj.lookup] = my_dict

    timing["3. second_loop"] = elapsed(this_start)
    timing["9. TOTAL"] = elapsed(start_time)

    return (response, timing)



class GeoRowMixin(object):
    @cached_property
    def country_iso2_display(self):
        if hasattr(self, "country_iso2"):
            return self.country_iso2
        else:
            return None

    @cached_property
    def country_iso3_display(self):
        if hasattr(self, "country_iso3"):
            return self.country_iso3
        else:
            return None

    @cached_property
    def continent_display(self):
        if hasattr(self, "continent"):
            return self.continent
        else:
            return None

    @cached_property
    def subcontinent_display(self):
        if hasattr(self, "subcontinent"):
            return self.subcontinent
        else:
            return None

    @cached_property
    def year_int(self):
        return int(self.year)

    @hybrid_property
    def bronze_gold_green_hybrid(self):
        return self.is_oa

    @hybrid_property
    def na(self):
        return self.num_distinct_articles


    def __repr__(self):
        return u"{} ({}, {})".format(self.__class__.__name__, self.lookup, self.year_int)


class OAMonitorUnpaywallByCountry(db.Model, GeoRowMixin):
    __tablename__ = 'oamonitor_unpaywall_by_country'
    __bind_key__ = "redshift_db"

    country = db.Column(db.Text, primary_key=True)
    country_iso2 = db.Column(db.Text)
    country_iso3 = db.Column(db.Text)
    subcontinent = db.Column(db.Text)
    continent = db.Column(db.Text)
    year = db.Column(db.Text, primary_key=True)
    num_distinct_articles = db.Column(db.Numeric)
    is_oa = deferred(db.Column(db.Numeric))
    bronze = deferred(db.Column(db.Numeric))
    green = deferred(db.Column(db.Numeric))
    gold = deferred(db.Column(db.Numeric))
    hybrid = deferred(db.Column(db.Numeric))
    bronze_green = deferred(db.Column(db.Numeric))
    bronze_gold = deferred(db.Column(db.Numeric))
    bronze_hybrid = deferred(db.Column(db.Numeric))
    green_gold = deferred(db.Column(db.Numeric))
    green_hybrid = deferred(db.Column(db.Numeric))
    gold_hybrid = deferred(db.Column(db.Numeric))
    bronze_green_gold = deferred(db.Column(db.Numeric))
    bronze_green_hybrid = deferred(db.Column(db.Numeric))
    bronze_gold_hybrid = deferred(db.Column(db.Numeric))
    green_gold_hybrid = deferred(db.Column(db.Numeric))

    @cached_property
    def lookup(self):
        return self.country

    def to_dict(self):
        return {
            "country": self.country,
            "year": self.year
        }

class OAMonitorUnpaywallBySubcontinent(db.Model, GeoRowMixin):
    __tablename__ = 'oamonitor_unpaywall_by_subcontinent'
    __bind_key__ = "redshift_db"

    subcontinent = db.Column(db.Text, primary_key=True)
    continent = db.Column(db.Text)
    year = db.Column(db.Text, primary_key=True)
    num_distinct_articles = db.Column(db.Numeric)
    is_oa = deferred(db.Column(db.Numeric))
    bronze = deferred(db.Column(db.Numeric))
    green = deferred(db.Column(db.Numeric))
    gold = deferred(db.Column(db.Numeric))
    hybrid = deferred(db.Column(db.Numeric))
    bronze_green = deferred(db.Column(db.Numeric))
    bronze_gold = deferred(db.Column(db.Numeric))
    bronze_hybrid = deferred(db.Column(db.Numeric))
    green_gold = deferred(db.Column(db.Numeric))
    green_hybrid = deferred(db.Column(db.Numeric))
    gold_hybrid = deferred(db.Column(db.Numeric))
    bronze_green_gold = deferred(db.Column(db.Numeric))
    bronze_green_hybrid = deferred(db.Column(db.Numeric))
    bronze_gold_hybrid = deferred(db.Column(db.Numeric))
    green_gold_hybrid = deferred(db.Column(db.Numeric))

    @cached_property
    def lookup(self):
        return self.subcontinent

    def to_dict(self):
        return {
            "subcontinent": self.subcontinent,
            "year": self.year
        }

class OAMonitorUnpaywallByContinent(db.Model, GeoRowMixin):
    __tablename__ = 'oamonitor_unpaywall_by_continent'
    __bind_key__ = "redshift_db"

    continent = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Text, primary_key=True)
    num_distinct_articles = db.Column(db.Numeric)
    is_oa = deferred(db.Column(db.Numeric))
    bronze = deferred(db.Column(db.Numeric))
    green = deferred(db.Column(db.Numeric))
    gold = deferred(db.Column(db.Numeric))
    hybrid = deferred(db.Column(db.Numeric))
    bronze_green = deferred(db.Column(db.Numeric))
    bronze_gold = deferred(db.Column(db.Numeric))
    bronze_hybrid = deferred(db.Column(db.Numeric))
    green_gold = deferred(db.Column(db.Numeric))
    green_hybrid = deferred(db.Column(db.Numeric))
    gold_hybrid = deferred(db.Column(db.Numeric))
    bronze_green_gold = deferred(db.Column(db.Numeric))
    bronze_green_hybrid = deferred(db.Column(db.Numeric))
    bronze_gold_hybrid = deferred(db.Column(db.Numeric))
    green_gold_hybrid = deferred(db.Column(db.Numeric))

    @cached_property
    def lookup(self):
        return self.continent

    def to_dict(self):
        return {
            "continent": self.continent,
            "year": self.year
        }

class OAMonitorUnpaywallWorldwide(db.Model, GeoRowMixin):
    __tablename__ = 'oamonitor_unpaywall_worldwide'
    __bind_key__ = "redshift_db"

    year = db.Column(db.Text, primary_key=True)
    num_distinct_articles = db.Column(db.Numeric)
    is_oa = deferred(db.Column(db.Numeric))
    bronze = deferred(db.Column(db.Numeric))
    green = deferred(db.Column(db.Numeric))
    gold = deferred(db.Column(db.Numeric))
    hybrid = deferred(db.Column(db.Numeric))
    bronze_green = deferred(db.Column(db.Numeric))
    bronze_gold = deferred(db.Column(db.Numeric))
    bronze_hybrid = deferred(db.Column(db.Numeric))
    green_gold = deferred(db.Column(db.Numeric))
    green_hybrid = deferred(db.Column(db.Numeric))
    gold_hybrid = deferred(db.Column(db.Numeric))
    bronze_green_gold = deferred(db.Column(db.Numeric))
    bronze_green_hybrid = deferred(db.Column(db.Numeric))
    bronze_gold_hybrid = deferred(db.Column(db.Numeric))
    green_gold_hybrid = deferred(db.Column(db.Numeric))

    @cached_property
    def lookup(self):
        return "global"

    def to_dict(self):
        return {
            "year": self.year
        }

def preload_global_objects():
    return OAMonitorUnpaywallWorldwide.query.options(undefer('*')).all()

# initial set is at the top of the file
global_objects = preload_global_objects()
