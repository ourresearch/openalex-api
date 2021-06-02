# long web timeout value needed to facilitate proxy of s3 changefile content
# setting to 10 hours: 60*60*10=36000
# web: gunicorn views:app -w 5 --timeout 36000 --reload
web: gunicorn views:app -w 2 --timeout 36000 --reload
warm_ricks_cache: python call_ricks_api.py --warm
run_random: python call_ricks_api.py
insights: python save_ip_insights.py
pmc: python save_pmc_metadata.py
crossref_2017: python save_crossref_in_db.py
warm_permissions_cache: python warm_cache.py

