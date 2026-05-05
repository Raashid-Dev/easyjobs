"""
fetch_jobs.py — EasyJobs multi-source job fetcher
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Sources (all free, no paid plan needed):
  1. Adzuna     — USA, Switzerland, Germany, UK, India, Singapore, AU, CA, FR
  2. Reed.co.uk — London / UK (free, 50 req/day)
  3. JSearch    — LinkedIn + Indeed globally incl. Dubai/Gulf
                  (free 200 req/month, runs once per day)
  4. RemoteOK   — 100% free, no auth, remote analytics/ops/backend roles
  5. Arbeitnow  — 100% free, no auth, Europe-focused tech roles
  6. Remotive   — 100% free, no auth, remote jobs worldwide
  7. Jobicy     — 100% free, no auth, remote jobs worldwide
  8. Himalayas  — 100% free, no auth, remote jobs with salary data
  9. JobSpy     — 100% free, scrapes Indeed directly (Python 3.11+ required)
 10. Apify      — OPTIONAL: LinkedIn + Google Jobs (costs money, skip if not needed)
"""

import os, re, json, time, base64, urllib.request, urllib.parse, ssl
from datetime import datetime, timezone, date

# ── SSL fix for Python 3.11+ on macOS (certifi CA bundle) ────────────────────
try:
    import certifi as _certifi
    _SSL_CTX = ssl.create_default_context(cafile=_certifi.where())
except Exception:
    _SSL_CTX = ssl.create_default_context()
    _SSL_CTX.check_hostname = False
    _SSL_CTX.verify_mode = ssl.CERT_NONE

def _urlopen(req, timeout=12):
    """urllib.request.urlopen wrapper that always uses our SSL context."""
    return urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX)

# Optional: JobSpy (python-jobspy package, Python 3.11+)
try:
    from jobspy import scrape_jobs as _jobspy_scrape
    JOBSPY_OK = True
except ImportError:
    JOBSPY_OK = False

BASE = os.path.dirname(os.path.abspath(__file__))

# ── API KEYS ──────────────────────────────────────────────────────────────────
ADZUNA_APP_ID  = os.getenv('ADZUNA_APP_ID',  '87bbd506')
ADZUNA_APP_KEY = os.getenv('ADZUNA_APP_KEY', 'fd79dfa64839c87f9bacae9dc3fde106')
REED_API_KEY   = os.getenv('REED_API_KEY',   '565ae00f-9e91-4e97-98a9-1bfa100d0ae1')
JSEARCH_KEY    = os.getenv('JSEARCH_KEY',    '4f20b35572msh47c8161a9c003d7p173df1jsn1bb4dc2fe02d')
APIFY_TOKEN    = os.getenv('APIFY_TOKEN',    '')

# ── SEARCH PLANS ──────────────────────────────────────────────────────────────
ADZUNA_SEARCHES = [
    # USA
    ('us','data analytics manager','USA'),
    ('us','digital analytics director','USA'),
    ('us','business intelligence manager','USA'),
    ('us','marketing analytics manager','USA'),
    ('us','senior analytics manager','USA'),
    ('us','web analytics manager','USA'),
    ('us','ga4 analytics','USA'),
    ('us','power bi developer','USA'),
    ('us','looker developer','USA'),
    ('us','ecommerce analytics','USA'),
    ('us','revenue operations manager','USA'),
    ('us','marketing operations manager','USA'),
    ('us','backend developer','USA'),
    ('us','data engineer','USA'),
    ('us','analytics engineer','USA'),
    ('us','full stack developer','USA'),
    # Switzerland
    ('ch','data analytics manager','Switzerland'),
    ('ch','business intelligence manager','Switzerland'),
    ('ch','digital analytics lead','Switzerland'),
    ('ch','senior data analyst','Switzerland'),
    ('ch','power bi analytics','Switzerland'),
    ('ch','web analytics','Switzerland'),
    ('ch','digital marketing analytics','Switzerland'),
    ('ch','marketing analytics manager','Switzerland'),
    ('ch','crm analytics','Switzerland'),
    ('ch','senior analytics manager','Switzerland'),
    ('ch','operations manager','Switzerland'),
    # Germany
    ('de','data analytics manager','Germany'),
    ('de','business intelligence lead','Germany'),
    ('de','digital analytics','Germany'),
    ('de','marketing analytics manager','Germany'),
    ('de','senior data analyst','Germany'),
    ('de','senior analytics manager','Germany'),
    ('de','operations manager','Germany'),
    ('de','backend developer','Germany'),
    ('de','data engineer','Germany'),
    # UK / London
    ('gb','data analytics manager london','UK'),
    ('gb','digital analytics director','UK'),
    ('gb','business intelligence manager','UK'),
    ('gb','marketing analytics london','UK'),
    ('gb','senior analytics manager','UK'),
    ('gb','ga4 analytics manager','UK'),
    ('gb','ecommerce analytics','UK'),
    ('gb','data visualization manager','UK'),
    ('gb','operations manager london','UK'),
    ('gb','revenue operations manager','UK'),
    ('gb','backend developer london','UK'),
    ('gb','data engineer','UK'),
    # India — Mumbai
    ('in','data analytics manager mumbai','India - Mumbai'),
    ('in','digital analytics lead mumbai','India - Mumbai'),
    ('in','operations manager mumbai','India - Mumbai'),
    ('in','backend developer mumbai','India - Mumbai'),
    ('in','data engineer mumbai','India - Mumbai'),
    # India — Delhi
    ('in','data analytics manager delhi','India - Delhi'),
    ('in','operations manager delhi','India - Delhi'),
    ('in','backend developer delhi','India - Delhi'),
    # India — Bangalore
    ('in','data analytics manager bangalore','India - Bangalore'),
    ('in','backend developer bangalore','India - Bangalore'),
    ('in','data engineer bangalore','India - Bangalore'),
    ('in','operations manager bangalore','India - Bangalore'),
    ('in','analytics engineer bangalore','India - Bangalore'),
    # India — general
    ('in','business intelligence manager','India'),
    ('in','marketing analytics','India'),
    ('in','senior analytics manager','India'),
    ('in','tableau developer','India'),
    ('in','crm analytics','India'),
    # Singapore
    ('sg','data analytics manager','Singapore'),
    ('sg','digital marketing analytics','Singapore'),
    ('sg','business intelligence','Singapore'),
    ('sg','senior analytics manager','Singapore'),
    ('sg','ecommerce analytics','Singapore'),
    ('sg','operations manager','Singapore'),
    ('sg','backend developer','Singapore'),
    ('sg','data engineer','Singapore'),
    # Australia
    ('au','data analytics manager','Australia'),
    ('au','digital analytics','Australia'),
    ('au','business intelligence manager','Australia'),
    ('au','senior analytics manager','Australia'),
    ('au','marketing analytics manager','Australia'),
    ('au','operations manager','Australia'),
    ('au','backend developer','Australia'),
    # Canada
    ('ca','data analytics manager','Canada'),
    ('ca','digital marketing analytics','Canada'),
    ('ca','senior analytics manager','Canada'),
    ('ca','ga4 analytics','Canada'),
    ('ca','operations manager','Canada'),
    ('ca','backend developer','Canada'),
    ('ca','data engineer','Canada'),
    # France
    ('fr','data analytics manager','France'),
    ('fr','digital marketing analytics','France'),
    ('fr','senior analytics manager','France'),
    # Netherlands
    ('nl','data analytics manager','Netherlands'),
    ('nl','business intelligence manager','Netherlands'),
    ('nl','digital analytics manager','Netherlands'),
    ('nl','senior analytics manager','Netherlands'),
    ('nl','data engineer','Netherlands'),
    ('nl','marketing analytics','Netherlands'),
    # Italy
    ('it','data analytics manager','Italy'),
    ('it','business intelligence manager','Italy'),
    ('it','senior analytics manager','Italy'),
    ('it','data engineer','Italy'),
    # Spain
    ('es','data analytics manager','Spain'),
    ('es','business intelligence manager','Spain'),
    ('es','digital analytics','Spain'),
    # Austria
    ('at','data analytics manager','Austria'),
    ('at','business intelligence manager','Austria'),
    ('at','senior analytics manager','Austria'),
    # Belgium
    ('be','data analytics manager','Belgium'),
    ('be','business intelligence manager','Belgium'),
    ('be','digital analytics manager','Belgium'),
    # ── FRESHER / ENTRY-LEVEL — all markets ──────────────────────────────
    ('gb','junior data analyst','UK'),
    ('gb','graduate analyst','UK'),
    ('gb','entry level data analyst','UK'),
    ('gb','junior hr executive','UK'),
    ('gb','graduate hr','UK'),
    ('gb','junior sales executive','UK'),
    ('gb','junior finance analyst','UK'),
    ('gb','graduate finance','UK'),
    ('gb','junior product analyst','UK'),
    ('gb','junior marketing executive','UK'),
    ('us','junior data analyst','USA'),
    ('us','entry level data analyst','USA'),
    ('us','junior business analyst','USA'),
    ('us','entry level marketing','USA'),
    ('us','junior finance analyst','USA'),
    ('us','entry level hr','USA'),
    ('us','entry level sales','USA'),
    ('us','junior product manager','USA'),
    ('in','data analyst fresher','India'),
    ('in','junior data analyst','India'),
    ('in','fresher analyst','India'),
    ('in','junior hr executive','India'),
    ('in','junior sales executive','India'),
    ('in','junior finance executive','India'),
    ('in','junior marketing executive','India'),
    ('in','junior product manager','India'),
    ('in','graduate trainee analyst','India'),
    ('in','entry level analyst','India'),
    ('sg','junior analyst','Singapore'),
    ('sg','graduate analyst','Singapore'),
    ('sg','entry level analyst','Singapore'),
    ('sg','junior hr','Singapore'),
    ('sg','junior sales','Singapore'),
    ('de','junior analyst','Germany'),
    ('de','junior data analyst','Germany'),
    ('de','graduate analyst','Germany'),
    ('au','junior analyst','Australia'),
    ('au','graduate analyst','Australia'),
    ('au','junior data analyst','Australia'),
    ('ca','junior analyst','Canada'),
    ('ca','junior data analyst','Canada'),
    ('ca','entry level analyst','Canada'),
    ('nl','junior analyst','Netherlands'),
    ('fr','junior analyst','France'),
    ('ch','junior analyst','Switzerland'),
    # ── NEW CATEGORIES ──────────────────────────────────────────────────
    # HR / Talent — UAE/Gulf focus via JSearch, but Adzuna for broader markets
    # (fresher variants already added in the FRESHER section above)
    ('gb','hr manager','UK'),
    ('gb','talent acquisition manager','UK'),
    ('gb','hr business partner','UK'),
    ('gb','people analytics manager','UK'),
    ('us','hr manager','USA'),
    ('us','talent acquisition manager','USA'),
    ('us','hr director','USA'),
    ('in','hr manager','India'),
    ('in','talent acquisition manager','India'),
    ('in','hr business partner','India'),
    ('sg','hr manager','Singapore'),
    ('de','hr manager','Germany'),
    # Sales / Business Development
    ('gb','sales manager','UK'),
    ('gb','business development manager','UK'),
    ('gb','account manager','UK'),
    ('us','sales manager','USA'),
    ('us','business development manager','USA'),
    ('us','account executive','USA'),
    ('in','sales manager','India'),
    ('in','business development manager','India'),
    ('sg','sales manager','Singapore'),
    ('de','sales manager','Germany'),
    ('au','sales manager','Australia'),
    # CRM / Customer Success
    ('gb','crm manager','UK'),
    ('gb','customer success manager','UK'),
    ('us','crm manager','USA'),
    ('us','customer success manager','USA'),
    ('us','salesforce manager','USA'),
    ('in','crm manager','India'),
    ('in','customer success manager','India'),
    ('sg','crm manager','Singapore'),
    # Retail / Category / E-commerce
    ('gb','store manager','UK'),
    ('gb','category manager retail','UK'),
    ('gb','ecommerce manager','UK'),
    ('us','store manager','USA'),
    ('us','category manager','USA'),
    ('us','ecommerce manager','USA'),
    ('in','store manager','India'),
    ('in','category manager','India'),
    ('in','ecommerce manager','India'),
    ('au','store manager','Australia'),
    ('de','category manager','Germany'),
    # Finance / FP&A / Treasury
    ('gb','finance manager','UK'),
    ('gb','financial analyst','UK'),
    ('gb','fp&a manager','UK'),
    ('gb','treasury manager','UK'),
    ('us','finance manager','USA'),
    ('us','financial planning analyst','USA'),
    ('us','fp&a manager','USA'),
    ('in','finance manager','India'),
    ('in','financial analyst','India'),
    ('in','fp&a manager','India'),
    ('ch','finance manager','Switzerland'),
    ('de','finance manager','Germany'),
    ('sg','finance manager','Singapore'),
    # Product Manager
    ('gb','product manager','UK'),
    ('us','product manager','USA'),
    ('us','product director','USA'),
    ('in','product manager','India'),
    ('sg','product manager','Singapore'),
    ('de','product manager','Germany'),
    # Marketing / Brand
    ('gb','marketing manager','UK'),
    ('gb','brand manager','UK'),
    ('us','marketing manager','USA'),
    ('us','brand manager','USA'),
    ('in','marketing manager','India'),
    ('in','brand manager','India'),
    # ── BLUE COLLAR — India — all major cities ────────────────────────────────
    # Mumbai
    ('in','driver job mumbai','India - Mumbai'),
    ('in','electrician job mumbai','India - Mumbai'),
    ('in','plumber mumbai','India - Mumbai'),
    ('in','ac technician mumbai','India - Mumbai'),
    ('in','carpenter job mumbai','India - Mumbai'),
    ('in','welder job mumbai','India - Mumbai'),
    ('in','tailor job mumbai','India - Mumbai'),
    ('in','cutting master mumbai','India - Mumbai'),
    ('in','maintenance technician mumbai','India - Mumbai'),
    ('in','mechanic job mumbai','India - Mumbai'),
    # Delhi / NCR
    ('in','driver job delhi','India - Delhi'),
    ('in','electrician job delhi','India - Delhi'),
    ('in','plumber job delhi','India - Delhi'),
    ('in','ac technician delhi','India - Delhi'),
    ('in','carpenter job delhi','India - Delhi'),
    ('in','welder job delhi','India - Delhi'),
    ('in','tailor job delhi','India - Delhi'),
    ('in','maintenance worker delhi','India - Delhi'),
    ('in','mechanic job delhi','India - Delhi'),
    # Bangalore
    ('in','driver job bangalore','India - Bangalore'),
    ('in','electrician job bangalore','India - Bangalore'),
    ('in','plumber job bangalore','India - Bangalore'),
    ('in','ac technician bangalore','India - Bangalore'),
    ('in','carpenter job bangalore','India - Bangalore'),
    ('in','maintenance technician bangalore','India - Bangalore'),
    # Chennai
    ('in','driver job chennai','India - Chennai'),
    ('in','electrician job chennai','India - Chennai'),
    ('in','plumber job chennai','India - Chennai'),
    ('in','welder job chennai','India - Chennai'),
    ('in','tailor job chennai','India - Chennai'),
    ('in','cutting master chennai','India - Chennai'),
    ('in','hvac technician chennai','India - Chennai'),
    # Hyderabad
    ('in','driver job hyderabad','India - Hyderabad'),
    ('in','electrician job hyderabad','India - Hyderabad'),
    ('in','plumber job hyderabad','India - Hyderabad'),
    ('in','ac technician hyderabad','India - Hyderabad'),
    ('in','carpenter hyderabad','India - Hyderabad'),
    # Pune
    ('in','driver job pune','India - Pune'),
    ('in','electrician job pune','India - Pune'),
    ('in','welder job pune','India - Pune'),
    ('in','maintenance technician pune','India - Pune'),
    # Ahmedabad / Surat (textile hub — tailors, cutting masters)
    ('in','tailor job ahmedabad','India - Ahmedabad'),
    ('in','cutting master ahmedabad','India - Ahmedabad'),
    ('in','garment job ahmedabad','India - Ahmedabad'),
    ('in','tailor job surat','India - Surat'),
    ('in','cutting master surat','India - Surat'),
    ('in','electrician job ahmedabad','India - Ahmedabad'),
    # General India (no city — catches all remaining)
    ('in','driver job','India'),
    ('in','electrician job','India'),
    ('in','plumber job','India'),
    ('in','ac technician job','India'),
    ('in','carpenter job','India'),
    ('in','welder job','India'),
    ('in','tailor job','India'),
    ('in','cutting master job','India'),
    ('in','maintenance technician job','India'),
    ('in','hvac technician','India'),
    ('in','mechanic job','India'),
    ('in','auto mechanic job','India'),
    ('in','vehicle technician','India'),
    ('in','tile fixer job','India'),
    ('in','mason job','India'),
    ('in','fabricator job','India'),
    ('in','spray painter job','India'),
    ('in','handyman job','India'),
    # ── BLUE COLLAR — UK (Adzuna) ─────────────────────────────────────────────
    ('gb','electrician job','UK'),
    ('gb','plumber job','UK'),
    ('gb','hvac engineer','UK'),
    ('gb','carpenter job','UK'),
    ('gb','welder job','UK'),
    ('gb','maintenance technician','UK'),
    ('gb','vehicle technician','UK'),
    ('gb','mechanic job','UK'),
    ('gb','tailor job london','UK'),
    # ── BLUE COLLAR — Australia ───────────────────────────────────────────────
    ('au','electrician job','Australia'),
    ('au','plumber job','Australia'),
    ('au','carpenter job','Australia'),
    ('au','welder job','Australia'),
    ('au','hvac technician','Australia'),
    ('au','maintenance technician','Australia'),
    ('au','mechanic job','Australia'),
    # ── BLUE COLLAR — Canada ──────────────────────────────────────────────────
    ('ca','electrician job','Canada'),
    ('ca','plumber job','Canada'),
    ('ca','hvac technician','Canada'),
    ('ca','carpenter job','Canada'),
    ('ca','welder job','Canada'),
    # ── BLUE COLLAR — Germany ─────────────────────────────────────────────────
    ('de','electrician job','Germany'),
    ('de','plumber job','Germany'),
    ('de','maintenance technician','Germany'),
    ('de','welder job','Germany'),
    # ── BLUE COLLAR — Singapore ───────────────────────────────────────────────
    ('sg','electrician job','Singapore'),
    ('sg','plumber job','Singapore'),
    ('sg','maintenance technician','Singapore'),
    ('sg','hvac technician','Singapore'),
    # ── BLUE COLLAR — New Zealand ─────────────────────────────────────────────
    ('nz','electrician job','New Zealand'),
    ('nz','plumber job','New Zealand'),
    ('nz','carpenter job','New Zealand'),
    ('nz','mechanic job','New Zealand'),
    ('nz','welder job','New Zealand'),
    # ── BLUE COLLAR — South Africa ────────────────────────────────────────────
    ('za','driver job','South Africa'),
    ('za','electrician job','South Africa'),
    ('za','plumber job','South Africa'),
    ('za','welder job','South Africa'),
    ('za','maintenance technician','South Africa'),
    ('za','mechanic job','South Africa'),
]
# Note: Ireland (ie), Sweden (se) not supported by Adzuna — covered via Arbeitnow

REED_SEARCHES = [
    ('data analytics manager','London'),
    ('business intelligence manager','London'),
    ('digital analytics manager','London'),
    ('marketing analytics','London'),
    ('web analytics manager','London'),
    ('data analytics manager','Manchester'),
    # New categories
    ('hr manager','London'),
    ('talent acquisition manager','London'),
    ('sales manager','London'),
    ('business development manager','London'),
    ('crm manager','London'),
    ('customer success manager','London'),
    ('finance manager','London'),
    ('fp&a manager','London'),
    ('product manager','London'),
    ('marketing manager','London'),
    ('store manager','London'),
    ('category manager','London'),
]

JSEARCH_SEARCHES = [
    # Analytics / BI — Gulf
    ('data analytics manager in Dubai UAE','AE'),
    ('digital marketing analytics manager Dubai','AE'),
    ('business intelligence manager Dubai UAE','AE'),
    ('data analytics manager Riyadh Saudi Arabia','SA'),
    ('senior analytics manager Abu Dhabi UAE','AE'),
    ('digital analytics lead Dubai UAE','AE'),
    ('data analytics manager Doha Qatar','QA'),
    ('business intelligence manager Qatar','QA'),
    ('analytics manager Kuwait City Kuwait','KW'),
    ('data analytics manager Manama Bahrain','BH'),
    # HR / Talent — Gulf
    ('hr manager Dubai UAE','AE'),
    ('talent acquisition manager Dubai','AE'),
    ('hr director Riyadh Saudi Arabia','SA'),
    ('people manager Dubai UAE','AE'),
    ('hr business partner Dubai','AE'),
    # Sales / BD — Gulf
    ('sales manager Dubai UAE','AE'),
    ('business development manager Dubai UAE','AE'),
    ('account manager Dubai UAE','AE'),
    ('sales director Riyadh Saudi Arabia','SA'),
    ('commercial manager Dubai UAE','AE'),
    # CRM / Customer Success — Gulf
    ('crm manager Dubai UAE','AE'),
    ('customer success manager Dubai UAE','AE'),
    ('salesforce manager Dubai UAE','AE'),
    # Retail / Category — Gulf
    ('store manager Dubai UAE','AE'),
    ('category manager Dubai UAE','AE'),
    ('retail manager Dubai UAE','AE'),
    ('ecommerce manager Dubai UAE','AE'),
    # Finance — Gulf
    ('finance manager Dubai UAE','AE'),
    ('financial analyst Dubai UAE','AE'),
    ('fp&a manager Dubai UAE','AE'),
    ('treasury manager Dubai UAE','AE'),
    ('finance director Riyadh Saudi Arabia','SA'),
    # Product / Marketing — Gulf
    ('product manager Dubai UAE','AE'),
    ('marketing manager Dubai UAE','AE'),
    ('brand manager Dubai UAE','AE'),
    ('growth manager Dubai UAE','AE'),
    # ── FRESHER / ENTRY-LEVEL — Gulf ────────────────────────────────────────
    ('junior data analyst Dubai UAE','AE'),
    ('graduate analyst Dubai UAE','AE'),
    ('entry level analyst Dubai','AE'),
    ('junior hr executive Dubai UAE','AE'),
    ('junior sales executive Dubai UAE','AE'),
    ('junior finance analyst Dubai UAE','AE'),
    ('junior marketing executive Dubai UAE','AE'),
    ('junior product manager Dubai UAE','AE'),
    ('fresher analyst Riyadh Saudi Arabia','SA'),
    ('graduate trainee Dubai UAE','AE'),
    ('associate analyst Dubai UAE','AE'),
    # ── IRELAND (not in Adzuna) ───────────────────────────────────────────────
    ('data analytics manager Dublin Ireland','IE'),
    ('analytics manager Ireland','IE'),
    ('digital analytics Dublin Ireland','IE'),
    ('business intelligence manager Ireland','IE'),
    ('hr manager Dublin Ireland','IE'),
    ('finance manager Dublin Ireland','IE'),
    ('sales manager Dublin Ireland','IE'),
    ('product manager Dublin Ireland','IE'),
    # ── HONG KONG ────────────────────────────────────────────────────────────
    ('data analytics manager Hong Kong','HK'),
    ('digital analytics manager Hong Kong','HK'),
    ('business intelligence manager Hong Kong','HK'),
    ('analytics manager Hong Kong','HK'),
    ('finance manager Hong Kong','HK'),
    # ── OMAN / BAHRAIN (Gulf expansion) ──────────────────────────────────────
    ('analytics manager Muscat Oman','OM'),
    ('hr manager Bahrain','BH'),
    ('finance manager Bahrain','BH'),
    # ── BLUE COLLAR — UAE / Dubai ─────────────────────────────────────────────
    ('driver job Dubai UAE','AE'),
    ('heavy driver Dubai UAE','AE'),
    ('electrician job Dubai UAE','AE'),
    ('plumber job Dubai UAE','AE'),
    ('ac technician job Dubai UAE','AE'),
    ('hvac technician Dubai UAE','AE'),
    ('maintenance technician Dubai UAE','AE'),
    ('carpenter job Dubai UAE','AE'),
    ('welder job Dubai UAE','AE'),
    ('fabricator job Dubai UAE','AE'),
    ('tailor job Dubai UAE','AE'),
    ('cutting master Dubai UAE','AE'),
    ('garment technician Dubai','AE'),
    ('mechanic job Dubai UAE','AE'),
    ('auto mechanic Dubai','AE'),
    ('painter job Dubai UAE','AE'),
    ('handyman job Dubai UAE','AE'),
    ('tile fixer Dubai UAE','AE'),
    ('mason job Dubai UAE','AE'),
    # ── BLUE COLLAR — UAE Abu Dhabi / Sharjah ────────────────────────────────
    ('electrician job Abu Dhabi UAE','AE'),
    ('plumber job Abu Dhabi UAE','AE'),
    ('driver job Abu Dhabi UAE','AE'),
    ('ac technician Abu Dhabi','AE'),
    ('carpenter job Abu Dhabi','AE'),
    ('welder job Abu Dhabi','AE'),
    ('maintenance technician Abu Dhabi','AE'),
    ('mechanic job Abu Dhabi UAE','AE'),
    ('electrician job Sharjah UAE','AE'),
    ('driver job Sharjah UAE','AE'),
    ('plumber job Sharjah','AE'),
    ('maintenance worker Sharjah','AE'),
    # ── BLUE COLLAR — Saudi Arabia ────────────────────────────────────────────
    ('driver job Riyadh Saudi Arabia','SA'),
    ('electrician job Riyadh Saudi Arabia','SA'),
    ('plumber job Saudi Arabia','SA'),
    ('maintenance worker Saudi Arabia','SA'),
    ('ac technician Saudi Arabia','SA'),
    ('welder job Saudi Arabia','SA'),
    ('carpenter job Saudi Arabia','SA'),
    ('hvac technician Saudi Arabia','SA'),
    ('mechanic job Riyadh','SA'),
    ('driver job Jeddah Saudi Arabia','SA'),
    ('electrician job Jeddah','SA'),
    ('plumber job Jeddah Saudi Arabia','SA'),
    ('tailor job Saudi Arabia','SA'),
    ('fabricator job Saudi Arabia','SA'),
    # ── BLUE COLLAR — Qatar ───────────────────────────────────────────────────
    ('driver job Qatar Doha','QA'),
    ('electrician job Qatar','QA'),
    ('plumber job Qatar','QA'),
    ('ac technician Qatar','QA'),
    ('welder job Qatar','QA'),
    ('maintenance technician Qatar','QA'),
    ('carpenter job Qatar','QA'),
    ('mechanic job Doha Qatar','QA'),
    # ── BLUE COLLAR — Kuwait / Bahrain / Oman ────────────────────────────────
    ('driver job Kuwait','KW'),
    ('electrician job Kuwait','KW'),
    ('maintenance technician Kuwait','KW'),
    ('plumber job Kuwait','KW'),
    ('welder job Kuwait','KW'),
    ('driver job Bahrain','BH'),
    ('electrician job Bahrain','BH'),
    ('maintenance technician Bahrain','BH'),
    ('driver job Oman Muscat','OM'),
    ('electrician job Oman','OM'),
    ('maintenance technician Oman','OM'),
    ('plumber job Oman','OM'),
    ('welder job Oman','OM'),
    # ── BLUE COLLAR — India (JSearch — LinkedIn/Indeed India) ─────────────────
    ('driver job Mumbai India','IN'),
    ('electrician job Mumbai India','IN'),
    ('plumber job Mumbai India','IN'),
    ('ac technician Mumbai India','IN'),
    ('carpenter job Mumbai India','IN'),
    ('welder job Mumbai India','IN'),
    ('driver job Delhi India','IN'),
    ('electrician job Delhi India','IN'),
    ('plumber job Delhi India','IN'),
    ('ac technician Delhi India','IN'),
    ('welder job Delhi India','IN'),
    ('mechanic job Delhi India','IN'),
    ('driver job Bangalore India','IN'),
    ('electrician job Bangalore India','IN'),
    ('maintenance technician Bangalore India','IN'),
    ('welder job Bangalore India','IN'),
    ('driver job Chennai India','IN'),
    ('electrician job Chennai India','IN'),
    ('plumber job Chennai India','IN'),
    ('welder job Chennai India','IN'),
    ('driver job Hyderabad India','IN'),
    ('electrician job Hyderabad India','IN'),
    ('ac technician Hyderabad India','IN'),
    ('maintenance technician Hyderabad India','IN'),
    ('driver job Pune India','IN'),
    ('electrician job Pune India','IN'),
    ('welder job Pune India','IN'),
    ('tailor job Ahmedabad India','IN'),
    ('cutting master Ahmedabad India','IN'),
    ('electrician job Ahmedabad India','IN'),
    ('tailor job Surat India','IN'),
    ('cutting master Surat India','IN'),
    ('driver job India','IN'),
    ('electrician job India','IN'),
    ('plumber job India','IN'),
    ('hvac technician India','IN'),
    ('carpenter job India','IN'),
    ('mechanic job India','IN'),
    # ── BLUE COLLAR — UK (JSearch via Indeed UK) ─────────────────────────────
    ('electrician job London UK','GB'),
    ('plumber job London UK','GB'),
    ('hvac engineer London','GB'),
    ('carpenter job UK','GB'),
    ('welder job UK','GB'),
    ('mechanic job UK','GB'),
    ('maintenance technician UK','GB'),
    ('driver job UK','GB'),
    # ── BLUE COLLAR — Australia (JSearch via Indeed AU) ───────────────────────
    ('electrician job Sydney Australia','AU'),
    ('plumber job Melbourne Australia','AU'),
    ('carpenter job Australia','AU'),
    ('welder job Australia','AU'),
    ('hvac technician Australia','AU'),
    ('mechanic job Australia','AU'),
    ('maintenance technician Australia','AU'),
]
JSEARCH_DAILY_FILE = os.path.join(BASE, 'data', 'jsearch_last_run.json')

# Apify search plans
APIFY_LI_SEARCHES = [
    # (keywords, location, fallback_country_code)
    ('data analytics manager',        'Dubai',              'AE'),
    ('business intelligence manager', 'Dubai',              'AE'),
    ('digital analytics manager',     'Dubai',              'AE'),
    ('marketing analytics manager',   'Dubai',              'AE'),
    ('analytics manager',             'Riyadh Saudi Arabia','SA'),
    ('data analytics manager',        'Doha Qatar',         'QA'),
    ('analytics manager',             'Kuwait City',        'KW'),
    ('data analytics manager',        'Singapore',          'sg'),
    ('analytics manager',             'Amsterdam Netherlands','nl'),
    ('data analytics manager',        'Dublin Ireland',     'ie'),
    ('data analytics director',       'London',             'gb'),
    ('marketing analytics manager',   'New York',           'us'),
    ('data analytics manager',        'Hong Kong',          'HK'),
]

APIFY_GJ_SEARCHES = [
    # (keywords, location, fallback_country_code)
    ('data analytics manager',        'Dubai UAE',    'AE'),
    ('analytics manager',             'Qatar',        'QA'),
    ('analytics manager',             'Kuwait',       'KW'),
    ('analytics manager',             'Singapore',    'sg'),
    ('data analytics manager',        'Netherlands',  'nl'),
    ('analytics manager',             'Ireland',      'ie'),
    ('data analytics manager',        'India',        'in'),
    ('digital analytics director',    'Germany',      'de'),
    ('analytics manager',             'Canada',       'ca'),
]

# ── PROFILE ───────────────────────────────────────────────────────────────────
MY_SKILLS = [
    'ga4','google analytics','gtm','google tag manager',
    'sql','bigquery','power bi','looker','looker studio','tableau',
    'data visualization','analytics','business intelligence',
    'digital analytics','web analytics','kpi','dashboard','reporting',
    'retail analytics','campaign analytics','dax','a/b testing',
    'crm analytics','rfm','clv','marketing analytics'
]
MY_EXP_YEARS = 10

# ── SPAM FILTER ───────────────────────────────────────────────────────────────
TITLE_KW = [
    # Analytics / BI / Data
    'analytic','analytics','data ','data-',' data','intelligence',' bi ',
    'dashboard','reporting','report','insight','kpi','digital marketing',
    'seo','sem','sql','power bi','tableau','looker','google analytics',
    'ga4','gtm','tag manager','marketing analytics','web analytics',
    'performance marketing','visualization','bigquery','measurement',
    'tracking','attribution','conversion rate','cro','mis ','information system',
    'business analyst','growth analyst','media analyst','performance analyst',
    # Operations
    'operations manager','revenue operations','marketing operations',
    'business operations','growth operations','operations analyst','ops manager','ops lead',
    'operations director','chief operating','head of operations',
    # Backend / Engineering
    'backend developer','backend engineer','full stack developer','fullstack developer',
    'data engineer','analytics engineer','software developer','python developer',
    'node.js developer','api developer','platform engineer','engineering manager',
    # HR / Talent / People
    'hr manager','hr director','hr business partner','hrbp','talent acquisition',
    'talent management','recruitment manager','recruiter','people manager',
    'people operations','human resources','workforce planning','learning & development',
    'l&d manager','compensation','benefits manager','hr analytics','people analytics',
    'head of hr','chief people','head of talent','hr lead',
    # CRM / Customer Success
    'crm manager','crm director','crm analyst','crm lead','crm specialist',
    'customer success manager','customer success director','salesforce','hubspot',
    'customer relationship','client success','account manager','client manager',
    'customer experience manager','cx manager',
    # Sales / Business Development
    'sales manager','sales director','head of sales','vp sales','vp of sales',
    'business development manager','business development director','bdm',
    'account executive','enterprise sales','sales analytics','sales operations',
    'commercial manager','commercial director','revenue manager',
    'regional sales','national sales','country manager',
    # Retail / E-commerce / Category
    'store manager','retail manager','retail director','head of retail',
    'category manager','category director','merchandising manager','merchandiser',
    'ecommerce manager','e-commerce manager','ecommerce director',
    'trade marketing','channel manager','omnichannel',
    'buying manager','buyer ','head of buying',
    # Finance / FP&A / Treasury
    'finance manager','finance director','head of finance','chief financial',
    'financial analyst','financial planning','fp&a','treasury manager',
    'financial controller','finance controller','corporate finance',
    'investment analyst','finance analyst','budgeting','forecasting',
    'financial reporting','management reporting',
    # Marketing / Brand / Growth
    'marketing manager','marketing director','head of marketing','cmo',
    'brand manager','brand director','growth manager','growth marketing',
    'digital marketing manager','performance marketing manager',
    'content marketing','email marketing manager',
    # Product
    'product manager','product director','head of product','vp product',
    'product analytics','product operations','product marketing',
    # Blue Collar / Trades
    'driver','heavy driver','light driver','truck driver','bus driver','cab driver',
    'electrician','electrical technician','electrical fitter',
    'plumber','plumbing technician',
    'ac technician','hvac technician','hvac engineer','refrigeration technician',
    'carpenter','woodworker','furniture maker',
    'welder','fabricator','fitter',
    'tailor','cutting master','pattern cutter','garment','stitching',
    'painter ','spray painter',
    'mechanic','auto mechanic','vehicle technician',
    'maintenance technician','maintenance worker','handyman',
    'mason','tile layer','flooring',
    # Fresher / Entry-level (any category)
    'junior analyst','junior data','junior hr','junior sales','junior finance',
    'junior marketing','junior product','graduate analyst','graduate trainee',
    'entry level analyst','entry level data','entry level hr','entry level sales',
    'fresher analyst','data analyst fresher','associate analyst','trainee analyst',
]
HARD_BLOCK = [
    'delivery boy','warehouse','nurse','teacher','security guard',
    'cook ','chef','cashier','receptionist',
    'telemarketing','domestic','factory worker','labourer',
    'solicitor','lawyer','legal counsel','construction manager',
    'pre construction','quantity surveyor','customer engineer',
    'market risk controller','java developer','kotlin developer',
    'cloud engineer','sap ewm','sap bpa',
    'generative ai engineer','associate (ca)',
]
SPAM_COMPANIES = ['testhiring','flat fee recruiter','wynwood tech']
DESC_KW = [
    'google analytics','ga4','power bi','tableau','looker','bigquery','sql',
    'dashboard','kpi','analytics','data analysis','reporting','digital marketing',
    'seo','tracking','attribution','conversion','insight','metric','measurement',
    'operations','backend','data engineer','revenue operations','python','api',
    # New category desc keywords
    'recruitment','talent acquisition','hr','human resources','salesforce','hubspot',
    'sales','business development','retail','store','ecommerce','crm',
    'finance','financial','treasury','fp&a','product manager','brand',
]

def is_spam(title, desc, company=''):
    t = title.lower().strip(); d = (desc or '').lower().strip(); c = (company or '').lower().strip()
    if not c or c == 'unknown': return True
    if len(d) < 40: return True
    if any(k in t for k in HARD_BLOCK): return True
    if any(k in c for k in SPAM_COMPANIES): return True
    title_hit = any(k in t for k in TITLE_KW)
    desc_hits = sum(1 for k in DESC_KW if k in d)
    if not title_hit and desc_hits < 2: return True
    return False

# ── HELPERS ───────────────────────────────────────────────────────────────────
def calc_fit(text, title=''):
    # Include both description and title in skill matching
    t = ((text or '') + ' ' + (title or '')).lower()
    matched = sum(1 for s in MY_SKILLS if s in t)
    skill_pct = min(100, int((matched / len(MY_SKILLS)) * 220))
    exp_match = 100 if MY_EXP_YEARS >= 8 else 70
    return min(98, max(40, int(skill_pct * 0.7 + exp_match * 0.3)))

def days_ago_from_iso(date_str):
    try:
        dt = datetime.fromisoformat((date_str or '').replace('Z','+00:00'))
        return max(0, (datetime.now(timezone.utc) - dt).days)
    except: return 0

def days_ago_from_dmy(date_str):
    try:
        dt = datetime.strptime(date_str, '%d/%m/%Y')
        return max(0, (datetime.now() - dt).days)
    except: return 0

def days_ago_from_ts(ts):
    try:
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        return max(0, (datetime.now(timezone.utc) - dt).days)
    except: return 0

CURRENCY = {
    'us':('USD',83,1.0),'US':('USD',83,1.0),'USD':('USD',83,1.0),
    'ch':('CHF',95,1.12),'de':('EUR',90,1.08),'gb':('GBP',105,1.25),
    'in':('INR',1,0.012),'sg':('SGD',62,0.74),'au':('AUD',55,0.65),
    'ca':('CAD',61,0.74),'fr':('EUR',90,1.08),
    'nl':('EUR',90,1.08),'NL':('EUR',90,1.08),
    'it':('EUR',90,1.08),'IT':('EUR',90,1.08),
    'es':('EUR',90,1.08),'ES':('EUR',90,1.08),
    'at':('EUR',90,1.08),'AT':('EUR',90,1.08),
    'ie':('EUR',90,1.08),'IE':('EUR',90,1.08),
    'se':('SEK',7,0.085),'SE':('SEK',7,0.085),
    'dk':('DKK',12,0.144),'DK':('DKK',12,0.144),
    'AE':('AED',23,0.272),'ae':('AED',23,0.272),
    'SA':('SAR',22,0.267),'sa':('SAR',22,0.267),
    'BH':('BHD',222,2.65),'bh':('BHD',222,2.65),
    'QA':('QAR',23,0.274),'qa':('QAR',23,0.274),
    'KW':('KWD',275,3.27),'kw':('KWD',275,3.27),
    'OM':('OMR',216,2.60),'om':('OMR',216,2.60),
    'HK':('HKD',11,0.128),'hk':('HKD',11,0.128),
}
COUNTRY_NAME = {
    'ch':'Switzerland','CH':'Switzerland','de':'Germany','DE':'Germany',
    'gb':'UK','GB':'UK','in':'India','IN':'India','sg':'Singapore','SG':'Singapore',
    'au':'Australia','AU':'Australia','ca':'Canada','CA':'Canada',
    'fr':'France','FR':'France','AE':'UAE','ae':'UAE',
    'SA':'Saudi Arabia','sa':'Saudi Arabia','BH':'Bahrain','bh':'Bahrain',
    'US':'USA','us':'USA',
    'nl':'Netherlands','NL':'Netherlands',
    'it':'Italy','IT':'Italy',
    'es':'Spain','ES':'Spain',
    'at':'Austria','AT':'Austria',
    'ie':'Ireland','IE':'Ireland',
    'se':'Sweden','SE':'Sweden',
    'dk':'Denmark','DK':'Denmark',
    'QA':'Qatar','qa':'Qatar',
    'KW':'Kuwait','kw':'Kuwait',
    'OM':'Oman','om':'Oman',
    'HK':'Hong Kong','hk':'Hong Kong',
}

def fmt_salary(lo, hi, cc):
    sym, inr_rate, usd_rate = CURRENCY.get(cc, ('USD',83,1.0))
    mid = (lo + hi) / 2 if hi > lo else lo
    inr_val = mid * inr_rate if mid else 0
    if inr_val >= 1e7:
        sal_inr = f"{inr_val / 1e7:.2f} CR"
    elif inr_val >= 1e5:
        sal_inr = f"{int(inr_val / 1e5)} L"
    else:
        sal_inr = 'Not disclosed'
    sal_loc = f"{sym} {int(lo):,} – {int(hi):,} / year" if hi > 0 else 'Not disclosed'
    sal_usd = int(mid * usd_rate) if mid else 0
    return sal_loc, sal_inr, sal_usd

def infer_category(title):
    t = title.lower()
    if any(k in t for k in ['hr manager','hr director','hrbp','hr business partner','talent acquisition',
                             'recruitment','recruiter','people manager','people operations','human resources',
                             'workforce','learning & development','l&d','compensation','hr analytics',
                             'people analytics','chief people','head of hr','head of talent']):
        return 'HR & Talent'
    if any(k in t for k in ['sales manager','sales director','head of sales','vp sales','account executive',
                             'business development','bdm','enterprise sales','regional sales','national sales',
                             'commercial manager','commercial director','country manager','revenue manager']):
        return 'Sales & BD'
    if any(k in t for k in ['store manager','retail manager','retail director','head of retail',
                             'category manager','merchandising','buying manager','buyer ','omnichannel',
                             'trade marketing','channel manager']):
        return 'Retail & E-commerce'
    if any(k in t for k in ['crm manager','crm director','crm analyst','crm lead','salesforce','hubspot',
                             'customer success','client success','cx manager','customer experience manager',
                             'account manager','client manager']):
        return 'CRM & Customer Success'
    if any(k in t for k in ['finance manager','finance director','head of finance','chief financial',
                             'financial analyst','financial planning','fp&a','treasury','financial controller',
                             'corporate finance','investment analyst','budgeting','forecasting',
                             'financial reporting','management reporting']):
        return 'Finance & FP&A'
    if any(k in t for k in ['product manager','product director','head of product','vp product',
                             'product analytics','product operations','product marketing']):
        return 'Product'
    if any(k in t for k in ['marketing manager','marketing director','head of marketing','cmo',
                             'brand manager','brand director','growth marketing','content marketing',
                             'email marketing','performance marketing manager','digital marketing manager']):
        return 'Marketing'
    if any(k in t for k in ['ecommerce manager','e-commerce manager','ecommerce director',
                             'growth manager','growth analyst']):
        return 'E-commerce & Growth'
    if any(k in t for k in ['operations manager','revenue operations','marketing operations',
                             'business operations','ops manager','ops lead','operations director',
                             'head of operations','chief operating']):
        return 'Operations'
    if any(k in t for k in ['backend developer','backend engineer','full stack','fullstack',
                             'data engineer','analytics engineer','software developer','python developer',
                             'node.js','api developer','engineering manager','platform engineer']):
        return 'Engineering'
    if any(k in t for k in ['digital marketing','seo','sem','campaign','paid media',
                             'performance marketing','social media']):
        return 'Digital Marketing'
    if any(k in t for k in [' bi ',' bi,','business intel','business intelligence']):
        return 'Business Intelligence'
    if any(k in t for k in ['web analytics','tag manager','gtm']):
        return 'Web Analytics'
    if any(k in t for k in ['driver','electrician','plumber','ac technician','hvac',
                             'carpenter','welder','fabricator','fitter','tailor',
                             'cutting master','pattern cutter','garment','stitching',
                             'painter ','mechanic','auto mechanic','maintenance technician',
                             'maintenance worker','handyman','mason','tile layer']):
        return 'Blue Collar'
    return 'Data Analytics'

def infer_experience(title, desc=''):
    """Return (label, min_years) based on title and description signals."""
    t = (title + ' ' + (desc or '')[:300]).lower()
    if any(k in t for k in ['fresher','fresh graduate','0-1 year','0 year',
                             'no experience required','entry level','entry-level',
                             'junior','graduate trainee','trainee','intern',
                             'apprentice','1-2 year','associate analyst']):
        return '0-2 years', 1
    if any(k in t for k in ['2-4 year','2-5 year','3-5 year','3-4 year',
                             'mid level','mid-level','2+ year','3+ year']):
        return '2-5 years', 3
    if any(k in t for k in ['10+ year','10-15 year','15+ year','head of',
                             'chief ','c-level','president','vp ','vice president']):
        return '10+ years', 10
    if any(k in t for k in ['5+ year','5-8 year','6+ year','7+ year','8+ year',
                             'senior','manager','director','lead ']):
        return '5+ years', 5
    return '3+ years', 3

def extract_phone(text):
    """Extract the first valid phone number from a text string."""
    if not text: return ''
    matches = re.findall(r'(?:\+?[\d][\d\s\-\.\(\)]{8,20}[\d])', text)
    for m in matches:
        digits = re.sub(r'\D', '', m)
        if 7 <= len(digits) <= 15 and not re.match(r'^(19|20)\d{2}', digits):
            return m.strip()
    return ''

def infer_work_mode(title, desc, is_remote=False):
    t = (title + ' ' + (desc or ''))[:500].lower()
    if is_remote or 'remote' in t or 'work from home' in t: return 'WFH'
    if 'hybrid' in t: return 'Hybrid'
    if 'freelance' in t or 'contract' in t: return 'Freelance'
    return 'On-site'

# ── 1. ADZUNA ─────────────────────────────────────────────────────────────────
ADZUNA_URL = 'https://api.adzuna.com/v1/api/jobs/{country}/search/1'

def norm_adzuna(hit, cc, city_hint=None):
    lo = hit.get('salary_min',0) or 0; hi = hit.get('salary_max',0) or 0
    sal_loc, sal_inr, sal_usd = fmt_salary(lo, hi, cc)
    desc = hit.get('description',''); title = hit.get('title','Analytics Role')
    co = hit.get('company',{}).get('display_name','Unknown')
    loc = hit.get('location',{}).get('display_name','')
    area = (hit.get('location',{}).get('area') or [''])[0]
    cat = hit.get('category',{}).get('label','Analytics')
    if is_spam(title, desc, co): return None
    cname = COUNTRY_NAME.get(cc, cc.upper())
    if city_hint and (not area or area.lower() in [cname.lower(), cc.lower(), '']):
        area = city_hint
    li_co = co.lower().replace(' ','-').replace('.','').replace(',','')
    exp_label, exp_min = infer_experience(title, desc)
    return {
        'id':f"az_{hit.get('id','')}",'title':title,'position_name':title,'company':co,
        'company_website':hit.get('redirect_url',''),'company_address':loc,
        'location':loc,'city':area or loc,'country':cname,
        'salary_local':sal_loc,'salary_inr_annual':sal_inr,'salary_usd_annual':sal_usd,
        'posted_date':hit.get('created','')[:10],
        'posted_days_ago':days_ago_from_iso(hit.get('created','')),
        'job_type':'Full-time','experience_required':exp_label,'experience_min':exp_min,
        'description':desc,'responsibilities':[],
        'skills_required':[s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have':[],
        'hr_contact':{'name':f'{co} HR','title':'Talent Acquisition','email':'','linkedin':f'https://www.linkedin.com/company/{li_co}/jobs/','phone':extract_phone(desc)},
        'has_phone': bool(extract_phone(desc)),
        'is_mnc':True,'company_size':'Not disclosed','company_size_category':'Unknown',
        'industry':cat,'category':infer_category(title),
        'work_mode':infer_work_mode(title,desc),'job_stability':4.0,
        'glassdoor_rating':4.0,'glassdoor_reviews':0,
        'apply_url':hit.get('redirect_url',''),'source':'Adzuna','tags':[cat,cname],
        'skills_match':[s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score':calc_fit(desc, title),
    }

def fetch_adzuna(cc, query, city_hint=None):
    params = urllib.parse.urlencode({'app_id':ADZUNA_APP_ID,'app_key':ADZUNA_APP_KEY,'results_per_page':20,'what':query,'sort_by':'date'})
    url = f"{ADZUNA_URL.format(country=cc)}?{params}"
    try:
        r = _urlopen(url, 15)
        hits = json.loads(r.read()).get('results',[])
        return [j for h in hits if (j := norm_adzuna(h, cc, city_hint=city_hint))]
    except Exception as e:
        print(f"    ✗ Adzuna [{cc}] '{query}': {e}"); return []

# ── 2. REED ───────────────────────────────────────────────────────────────────
REED_URL = 'https://www.reed.co.uk/api/1.0/search'

def norm_reed(hit):
    lo = float(hit.get('minimumSalary') or 0); hi = float(hit.get('maximumSalary') or 0)
    sal_loc, sal_inr, sal_usd = fmt_salary(lo, hi, 'gb')
    title = hit.get('jobTitle','Analytics Role'); co = hit.get('employerName','Unknown')
    loc = hit.get('locationName','UK')
    desc = strip_html(hit.get('jobDescription',''))
    if is_spam(title, desc, co): return None
    li_co = co.lower().replace(' ','-').replace('.','').replace(',','')
    exp_label, exp_min = infer_experience(title, desc)
    return {
        'id':f"rd_{hit.get('jobId','')}",'title':title,'position_name':title,'company':co,
        'company_website':hit.get('jobUrl',''),'company_address':loc,
        'location':loc,'city':loc,'country':'UK',
        'salary_local':sal_loc,'salary_inr_annual':sal_inr,'salary_usd_annual':sal_usd,
        'posted_date':(hit.get('date','') or '')[:10],
        'posted_days_ago':days_ago_from_dmy(hit.get('date','')) if hit.get('date') else 0,
        'job_type':'Full-time','experience_required':exp_label,'experience_min':exp_min,
        'description':desc,'responsibilities':[],
        'skills_required':[s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have':[],
        'hr_contact':{'name':f'{co} Talent Team','title':'Talent Acquisition','email':'','linkedin':f'https://www.linkedin.com/company/{li_co}/jobs/','phone':extract_phone(desc)},
        'has_phone': bool(extract_phone(desc)),
        'is_mnc':False,'company_size':'Not disclosed','company_size_category':'Unknown',
        'industry':'Analytics','category':infer_category(title),
        'work_mode':infer_work_mode(title,desc),'job_stability':3.8,
        'glassdoor_rating':3.8,'glassdoor_reviews':0,
        'apply_url':hit.get('jobUrl',''),'source':'Reed.co.uk','tags':['UK','London','Analytics'],
        'skills_match':[s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score':calc_fit(desc, title),
    }

def fetch_reed(keywords, location):
    creds = base64.b64encode(f"{REED_API_KEY}:".encode()).decode()
    params = urllib.parse.urlencode({'keywords':keywords,'locationName':location,'resultsToTake':50,'fullTime':'true'})
    try:
        req = urllib.request.Request(f"{REED_URL}?{params}", headers={'Authorization':f'Basic {creds}'})
        r = _urlopen(req, 15)
        hits = json.loads(r.read()).get('results',[])
        return [j for h in hits if (j := norm_reed(h))]
    except Exception as e:
        print(f"    ✗ Reed '{keywords}' / '{location}': {e}"); return []

# ── 3. JSEARCH ────────────────────────────────────────────────────────────────
JSEARCH_URL = 'https://jsearch.p.rapidapi.com/search'

def jsearch_already_ran_today():
    if not os.path.exists(JSEARCH_DAILY_FILE): return False
    try:
        with open(JSEARCH_DAILY_FILE) as f: return json.load(f).get('date') == str(date.today())
    except: return False

def mark_jsearch_ran():
    os.makedirs(os.path.dirname(JSEARCH_DAILY_FILE), exist_ok=True)
    with open(JSEARCH_DAILY_FILE, 'w') as f:
        json.dump({'date':str(date.today()),'ts':datetime.now().isoformat()}, f)

def norm_jsearch(hit):
    title = hit.get('job_title') or 'Analytics Role'; co = hit.get('employer_name') or 'Unknown'
    desc = hit.get('job_description') or ''; city = hit.get('job_city') or ''
    cc = hit.get('job_country') or 'AE'; url = hit.get('job_apply_link') or ''
    pub = hit.get('job_publisher') or 'Indeed'; ts = hit.get('job_posted_at_timestamp') or 0
    remote = hit.get('job_is_remote') or False
    lo = hit.get('job_min_salary') or 0; hi = hit.get('job_max_salary') or 0
    per = hit.get('job_salary_period') or 'YEAR'; cur = hit.get('job_salary_currency') or ''
    if per == 'MONTH': lo,hi = lo*12,hi*12
    if per == 'HOUR':  lo,hi = lo*2080,hi*2080
    cur_map = {'AED':'AE','SAR':'SA','BHD':'BH','SGD':'sg','GBP':'gb','EUR':'de','USD':'USD','INR':'in','AUD':'au','CAD':'ca'}
    sal_loc, sal_inr, sal_usd = fmt_salary(lo, hi, cur_map.get(cur, cc))
    if is_spam(title, desc, co): return None
    cname = COUNTRY_NAME.get(cc) or COUNTRY_NAME.get(cc.lower()) or COUNTRY_NAME.get(cc.upper()) or cc
    if not cname or len(cname) <= 2: cname = 'Unknown'
    # Default city by country code when JSearch returns blank city
    DEFAULT_CITY = {'AE':'Dubai','SA':'Riyadh','QA':'Doha','KW':'Kuwait City',
                    'BH':'Manama','OM':'Muscat','SG':'Singapore','IN':'India','GB':'London',
                    'DE':'Germany','AU':'Australia','CA':'Canada'}
    city = city or DEFAULT_CITY.get(cc.upper(), '')
    li_co = co.lower().replace(' ','-').replace('.','').replace(',','')
    exp_label, exp_min = infer_experience(title, desc)
    return {
        'id':f"js_{abs(hash(url+title))}",'title':title,'position_name':title,'company':co,
        'company_website':hit.get('employer_website',''),'company_address':f"{city}, {cname}",
        'location':f"{city}, {cname}",'city':city or cname,'country':cname,
        'salary_local':sal_loc,'salary_inr_annual':sal_inr,'salary_usd_annual':sal_usd,
        'posted_date':datetime.fromtimestamp(ts).strftime('%Y-%m-%d') if ts else '',
        'posted_days_ago':days_ago_from_ts(ts) if ts else 0,
        'job_type':'Full-time','experience_required':exp_label,'experience_min':exp_min,
        'description':desc[:1000],'responsibilities':hit.get('job_highlights',{}).get('Responsibilities',[])[:6],
        'skills_required':(hit.get('job_required_skills') or [])[:8] or [s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have':[],
        'hr_contact':{'name':f'{co} Recruiter','title':'Talent Acquisition','email':'','linkedin':f'https://www.linkedin.com/company/{li_co}/jobs/','phone':extract_phone(desc)},
        'has_phone': bool(extract_phone(desc)),
        'is_mnc':True,'company_size':'Not disclosed','company_size_category':'Unknown',
        'industry':'Analytics','category':infer_category(title),
        'work_mode':infer_work_mode(title,desc,remote),'job_stability':4.0,
        'glassdoor_rating':4.0,'glassdoor_reviews':0,
        'apply_url':url,'source':f'JSearch ({pub})','tags':[cname,'Analytics'],
        'skills_match':[s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score':calc_fit(desc, title),
    }

def fetch_jsearch(query, cc):
    params = urllib.parse.urlencode({'query':query,'page':'1','num_pages':'1','date_posted':'month'})
    try:
        req = urllib.request.Request(f"{JSEARCH_URL}?{params}", headers={
            'x-rapidapi-host':'jsearch.p.rapidapi.com','x-rapidapi-key':JSEARCH_KEY,'Content-Type':'application/json'})
        r = _urlopen(req, 15)
        d = json.loads(r.read())
        if d.get('status') != 'OK': print(f"    ✗ JSearch '{query}': {d.get('status')}"); return []
        return [j for h in d.get('data',[]) if (j := norm_jsearch(h))]
    except Exception as e:
        err = str(e)
        if '429' in err: return None   # signal quota exhausted to caller
        if '403' in err: print(f"    ✗ JSearch: API key not subscribed")
        else: print(f"    ✗ JSearch '{query}': {e}")
        return []

# ── 4. REMOTEOK (free, no auth) ───────────────────────────────────────────────
REMOTEOK_URL = 'https://remoteok.com/api'
REMOTEOK_TAGS = [
    'analytics','business-intelligence','data','marketing','operations',
    'growth','seo','crm','reporting','backend','python','sql',
]

def strip_html(text):
    """Remove HTML tags and decode common entities from a string."""
    if not text: return ''
    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = clean.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>') \
                 .replace('&nbsp;', ' ').replace('&#39;', "'").replace('&quot;', '"')
    clean = re.sub(r'\s{2,}', ' ', clean).strip()
    return clean

def norm_remoteok(hit):
    title   = (hit.get('position') or '').strip()
    co      = (hit.get('company') or 'Unknown').strip()
    desc    = strip_html((hit.get('description') or '').strip())
    url     = (hit.get('url') or '').strip()
    sal_lo  = int(hit.get('salary_min') or 0)
    sal_hi  = int(hit.get('salary_max') or 0)
    pub_ts  = hit.get('epoch') or 0

    if not title or is_spam(title, desc, co): return None
    sal_loc, sal_inr, sal_usd = fmt_salary(sal_lo, sal_hi, 'us')
    li_co = co.lower().replace(' ', '-').replace('.', '').replace(',', '')
    exp_label, exp_min = infer_experience(title, desc)
    return {
        'id': f"ro_{hit.get('id', abs(hash(url+title)))}", 'title': title, 'position_name': title, 'company': co,
        'company_website': url, 'company_address': 'Remote',
        'location': 'Remote', 'city': 'Remote', 'country': 'USA',
        'salary_local': sal_loc, 'salary_inr_annual': sal_inr, 'salary_usd_annual': sal_usd,
        'posted_date': '', 'posted_days_ago': days_ago_from_ts(pub_ts),
        'job_type': 'Full-time', 'experience_required': exp_label, 'experience_min': exp_min,
        'description': desc[:1000], 'responsibilities': [],
        'skills_required': [s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have': [],
        'hr_contact': {'name': f'{co} Hiring', 'title': 'Talent Acquisition', 'email': '',
                       'linkedin': f'https://www.linkedin.com/company/{li_co}/jobs/', 'phone': extract_phone(desc)},
        'has_phone': bool(extract_phone(desc)),
        'is_mnc': False, 'company_size': 'Not disclosed', 'company_size_category': 'Unknown',
        'industry': 'Analytics', 'category': infer_category(title),
        'work_mode': 'WFH', 'job_stability': 3.8,
        'glassdoor_rating': 3.8, 'glassdoor_reviews': 0,
        'apply_url': url, 'source': 'RemoteOK', 'tags': ['Remote', 'USA'],
        'skills_match': [s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score': calc_fit(desc, title),
    }

def fetch_remoteok():
    """Fetch all jobs from RemoteOK (free API, no key needed) and keep relevant ones."""
    print("  Fetching RemoteOK …", end='', flush=True)
    try:
        req = urllib.request.Request(REMOTEOK_URL, headers={'User-Agent': 'EasyJobs/1.0'})
        r   = _urlopen(req, 20)
        raw = json.loads(r.read())
        # first element is a meta-info dict, skip it
        hits = [h for h in raw if isinstance(h, dict) and h.get('position')]
        jobs = [j for h in hits if (j := norm_remoteok(h))]
        print(f" {len(jobs)} jobs (from {len(hits)} listings)")
        return jobs
    except Exception as e:
        print(f"\n    ✗ RemoteOK: {e}"); return []

# ── 5. ARBEITNOW (free, no auth — Europe-focused) ─────────────────────────────
ARBEITNOW_URL  = 'https://www.arbeitnow.com/api/job-board-api'
ARBEITNOW_TAGS = ['analytics','data','business-intelligence','marketing','operations','backend','python',
                  'sales','hr','finance','product','crm','ecommerce','retail',
                  'electrician','plumber','carpenter','welder','maintenance','hvac','mechanic']

def norm_arbeitnow(hit):
    title  = (hit.get('title') or '').strip()
    co     = (hit.get('company_name') or 'Unknown').strip()
    desc   = (hit.get('description') or '').strip()
    url    = (hit.get('url') or '').strip()
    loc    = (hit.get('location') or 'Europe').strip()
    remote = bool(hit.get('remote'))
    pub_ts = hit.get('created_at') or 0

    if not title or is_spam(title, desc, co): return None
    cc    = _guess_country(loc) or 'de'
    cname = COUNTRY_NAME.get(cc, cc.upper())
    city  = loc.split(',')[0].strip() if loc else cname
    sal_loc, sal_inr, sal_usd = fmt_salary(0, 0, cc)
    wm    = infer_work_mode(title, desc, remote)
    li_co = co.lower().replace(' ', '-').replace('.', '').replace(',', '')
    exp_label, exp_min = infer_experience(title, desc)
    return {
        'id': f"an_{abs(hash(url+title+co))}", 'title': title, 'position_name': title, 'company': co,
        'company_website': url, 'company_address': loc,
        'location': loc, 'city': city, 'country': cname,
        'salary_local': sal_loc, 'salary_inr_annual': sal_inr, 'salary_usd_annual': sal_usd,
        'posted_date': '', 'posted_days_ago': days_ago_from_ts(pub_ts),
        'job_type': 'Full-time', 'experience_required': exp_label, 'experience_min': exp_min,
        'description': desc[:1000], 'responsibilities': [],
        'skills_required': [s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have': [],
        'hr_contact': {'name': f'{co} Hiring', 'title': 'Talent Acquisition', 'email': '',
                       'linkedin': f'https://www.linkedin.com/company/{li_co}/jobs/', 'phone': extract_phone(desc)},
        'has_phone': bool(extract_phone(desc)),
        'is_mnc': False, 'company_size': 'Not disclosed', 'company_size_category': 'Unknown',
        'industry': 'Analytics', 'category': infer_category(title),
        'work_mode': wm, 'job_stability': 3.8,
        'glassdoor_rating': 3.8, 'glassdoor_reviews': 0,
        'apply_url': url, 'source': 'Arbeitnow', 'tags': [cname, 'Europe'],
        'skills_match': [s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score': calc_fit(desc, title),
    }

def fetch_arbeitnow():
    """Fetch jobs from Arbeitnow (free API, no key needed) — Europe tech roles."""
    jobs_all = []
    for tag in ARBEITNOW_TAGS:
        print(f"  [{tag}] …", end='', flush=True)
        try:
            params = urllib.parse.urlencode({'tags': tag})
            req    = urllib.request.Request(f"{ARBEITNOW_URL}?{params}",
                                            headers={'User-Agent': 'EasyJobs/1.0'})
            r      = _urlopen(req, 15)
            hits   = json.loads(r.read()).get('data', [])
            normed = [j for h in hits if (j := norm_arbeitnow(h))]
            jobs_all.extend(normed)
            print(f" {len(normed)}")
        except Exception as e:
            print(f"\n    ✗ Arbeitnow [{tag}]: {e}")
        time.sleep(0.3)
    return jobs_all

# ── 6. REMOTIVE (free, no auth — remote jobs worldwide) ──────────────────────
REMOTIVE_URL        = 'https://remotive.com/api/remote-jobs'
REMOTIVE_CATEGORIES = ['data', 'marketing', 'business', 'finance', 'management', 'hr', 'sales']

def norm_remotive(hit):
    title   = (hit.get('title') or '').strip()
    co      = (hit.get('company_name') or 'Unknown').strip()
    desc    = strip_html((hit.get('description') or '').strip())
    url     = (hit.get('url') or '').strip()
    sal_raw = hit.get('salary') or ''
    pub     = hit.get('publication_date') or ''

    if not title or is_spam(title, desc, co): return None

    sal_loc, sal_inr, sal_usd = _extract_salary(sal_raw, 'us') if sal_raw else ('Not disclosed', 'Not disclosed', 0)
    exp_label, exp_min = infer_experience(title, desc)
    li_co = co.lower().replace(' ', '-').replace('.', '').replace(',', '')
    pda = days_ago_from_iso(pub) if pub else 0

    return {
        'id': f"rm_{abs(hash(url+title+co))}", 'title': title, 'position_name': title, 'company': co,
        'company_website': hit.get('company_url', url), 'company_address': 'Remote',
        'location': 'Remote', 'city': 'Remote', 'country': 'USA',
        'salary_local': sal_loc, 'salary_inr_annual': sal_inr, 'salary_usd_annual': sal_usd,
        'posted_date': pub[:10] if pub else '', 'posted_days_ago': pda,
        'job_type': 'Full-time', 'experience_required': exp_label, 'experience_min': exp_min,
        'description': desc[:1000], 'responsibilities': [],
        'skills_required': [s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have': [],
        'hr_contact': {'name': f'{co} Hiring', 'title': 'Talent Acquisition', 'email': '',
                       'linkedin': f'https://www.linkedin.com/company/{li_co}/jobs/', 'phone': extract_phone(desc)},
        'has_phone': bool(extract_phone(desc)),
        'is_mnc': False, 'company_size': 'Not disclosed', 'company_size_category': 'Unknown',
        'industry': 'Tech', 'category': infer_category(title),
        'work_mode': 'WFH', 'job_stability': 3.8,
        'glassdoor_rating': 3.8, 'glassdoor_reviews': 0,
        'apply_url': url, 'source': 'Remotive', 'tags': ['Remote', 'USA'],
        'skills_match': [s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score': calc_fit(desc, title),
    }

def fetch_remotive():
    """Fetch jobs from Remotive (free API, no key needed) — remote jobs worldwide."""
    jobs_all = []
    for cat in REMOTIVE_CATEGORIES:
        print(f"  [{cat}] …", end='', flush=True)
        try:
            params = urllib.parse.urlencode({'category': cat, 'limit': 50})
            req    = urllib.request.Request(f"{REMOTIVE_URL}?{params}",
                                            headers={'User-Agent': 'EasyJobs/1.0'})
            r      = _urlopen(req, 15)
            hits   = json.loads(r.read()).get('jobs', [])
            normed = [j for h in hits if (j := norm_remotive(h))]
            jobs_all.extend(normed)
            print(f" {len(normed)}")
        except Exception as e:
            print(f"\n    ✗ Remotive [{cat}]: {e}")
        time.sleep(0.3)
    return jobs_all

# ── 7. JOBICY (free, no auth — remote jobs worldwide) ─────────────────────────
JOBICY_URL  = 'https://jobicy.com/api/v2/remote-jobs'
JOBICY_TAGS = ['analytics', 'marketing', 'finance', 'sales',
               'operations', 'product', 'recruiting', 'management']

def norm_jobicy(hit):
    title   = (hit.get('jobTitle') or '').strip()
    co      = (hit.get('companyName') or 'Unknown').strip()
    desc    = strip_html((hit.get('jobDescription') or '').strip())
    url     = (hit.get('url') or '').strip()
    sal_lo  = int(hit.get('annualSalaryMin') or 0)
    sal_hi  = int(hit.get('annualSalaryMax') or 0)
    pub     = hit.get('pubDate') or ''
    geo     = hit.get('jobGeo') or 'Worldwide'

    if not title or is_spam(title, desc, co): return None

    cc      = _guess_country(geo) or 'us'
    cname   = COUNTRY_NAME.get(cc, 'USA')
    sal_loc, sal_inr, sal_usd = fmt_salary(sal_lo, sal_hi, cc)
    exp_label, exp_min = infer_experience(title, desc)
    li_co   = co.lower().replace(' ', '-').replace('.', '').replace(',', '')
    pda     = days_ago_from_iso(pub) if pub else 0

    return {
        'id': f"jy_{abs(hash(url+title+co))}", 'title': title, 'position_name': title, 'company': co,
        'company_website': url, 'company_address': geo,
        'location': geo, 'city': geo, 'country': cname,
        'salary_local': sal_loc, 'salary_inr_annual': sal_inr, 'salary_usd_annual': sal_usd,
        'posted_date': pub[:10] if pub else '', 'posted_days_ago': pda,
        'job_type': (hit.get('jobType') or 'Full-time'),
        'experience_required': exp_label, 'experience_min': exp_min,
        'description': desc[:1000], 'responsibilities': [],
        'skills_required': [s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have': [],
        'hr_contact': {'name': f'{co} Hiring', 'title': 'Talent Acquisition', 'email': '',
                       'linkedin': f'https://www.linkedin.com/company/{li_co}/jobs/', 'phone': extract_phone(desc)},
        'has_phone': bool(extract_phone(desc)),
        'is_mnc': False, 'company_size': 'Not disclosed', 'company_size_category': 'Unknown',
        'industry': 'Tech', 'category': infer_category(title),
        'work_mode': 'WFH', 'job_stability': 3.8,
        'glassdoor_rating': 3.8, 'glassdoor_reviews': 0,
        'apply_url': url, 'source': 'Jobicy', 'tags': ['Remote', cname],
        'skills_match': [s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score': calc_fit(desc, title),
    }

def fetch_jobicy():
    """Fetch jobs from Jobicy (free API, no key needed) — remote jobs worldwide."""
    jobs_all = []
    for tag in JOBICY_TAGS:
        print(f"  [{tag}] …", end='', flush=True)
        try:
            params = urllib.parse.urlencode({'count': 50, 'tag': tag})
            req    = urllib.request.Request(f"{JOBICY_URL}?{params}",
                                            headers={'User-Agent': 'EasyJobs/1.0'})
            r      = _urlopen(req, 15)
            hits   = json.loads(r.read()).get('jobs', [])
            normed = [j for h in hits if (j := norm_jobicy(h))]
            jobs_all.extend(normed)
            print(f" {len(normed)}")
        except Exception as e:
            print(f"\n    ✗ Jobicy [{tag}]: {e}")
        time.sleep(0.5)
    return jobs_all

# ── 8. HIMALAYAS (free, no auth — remote jobs with salary data) ───────────────
HIMALAYAS_URL  = 'https://himalayas.app/jobs/api'
HIMALAYAS_CATS = ['analytics','marketing','finance','operations','product',
                  'sales','human-resources','business-development']

def _parse_list_field(val):
    """Parse a stringified Python list like \"['Senior', 'Mid']\" → list."""
    if not val: return []
    if isinstance(val, list): return val
    try:
        import ast
        return ast.literal_eval(str(val))
    except Exception:
        return [str(val).strip("[]'\" ")]

def _himalayas_currency_cc(currency):
    mapping = {'USD':'us','GBP':'gb','EUR':'de','AUD':'au','CAD':'ca',
               'SGD':'sg','CHF':'ch','AED':'AE','INR':'in','HKD':'HK'}
    return mapping.get((currency or '').upper(), 'us')

def norm_himalayas(hit):
    title    = (hit.get('title') or '').strip()
    co       = (hit.get('companyName') or 'Unknown').strip()
    desc     = strip_html((hit.get('description') or hit.get('excerpt') or '').strip())
    url      = (hit.get('applicationLink') or hit.get('guid') or '').strip()
    sal_lo   = int(hit.get('minSalary') or 0)
    sal_hi   = int(hit.get('maxSalary') or 0)
    currency = (hit.get('currency') or 'USD').upper()
    pub_ts   = hit.get('pubDate') or 0
    locs     = _parse_list_field(hit.get('locationRestrictions'))
    job_type = (hit.get('employmentType') or 'Full Time').replace('_', ' ').title()

    if not title or is_spam(title, desc, co): return None

    cc     = _himalayas_currency_cc(currency)
    cname  = COUNTRY_NAME.get(cc, 'USA')
    # Use location restriction as location label
    loc_label = locs[0] if locs else 'Remote'
    if loc_label.lower() in ('worldwide', 'anywhere', ''): loc_label = 'Remote'

    sal_loc, sal_inr, sal_usd = fmt_salary(sal_lo, sal_hi, cc)
    exp_label, exp_min = infer_experience(title, desc)
    li_co  = co.lower().replace(' ', '-').replace('.', '').replace(',', '')
    pda    = days_ago_from_ts(pub_ts) if pub_ts else 0

    return {
        'id': f"hm_{abs(hash(url+title+co))}", 'title': title, 'position_name': title, 'company': co,
        'company_website': f'https://himalayas.app/companies/{hit.get("companySlug","")}',
        'company_address': loc_label,
        'location': loc_label, 'city': loc_label, 'country': cname,
        'salary_local': sal_loc, 'salary_inr_annual': sal_inr, 'salary_usd_annual': sal_usd,
        'posted_date': '', 'posted_days_ago': pda,
        'job_type': job_type,
        'experience_required': exp_label, 'experience_min': exp_min,
        'description': desc[:1000], 'responsibilities': [],
        'skills_required': [s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have': [],
        'hr_contact': {'name': f'{co} Hiring', 'title': 'Talent Acquisition', 'email': '',
                       'linkedin': f'https://www.linkedin.com/company/{li_co}/jobs/', 'phone': extract_phone(desc)},
        'has_phone': bool(extract_phone(desc)),
        'is_mnc': False, 'company_size': 'Not disclosed', 'company_size_category': 'Unknown',
        'industry': 'Tech', 'category': infer_category(title),
        'work_mode': 'WFH', 'job_stability': 3.8,
        'glassdoor_rating': 3.8, 'glassdoor_reviews': 0,
        'apply_url': url, 'source': 'Himalayas', 'tags': ['Remote', cname],
        'skills_match': [s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score': calc_fit(desc, title),
    }

def fetch_himalayas():
    """Fetch jobs from Himalayas (free API, no key needed) — remote jobs with salary."""
    jobs_all = []
    for cat in HIMALAYAS_CATS:
        print(f"  [{cat}] …", end='', flush=True)
        cat_jobs = []
        for offset in [0, 20, 40]:   # up to 60 jobs per category
            try:
                params = urllib.parse.urlencode({'limit': 20, 'offset': offset, 'categories': cat})
                req    = urllib.request.Request(f"{HIMALAYAS_URL}?{params}",
                                                headers={'User-Agent': 'EasyJobs/1.0',
                                                         'Accept': 'application/json'})
                r      = _urlopen(req, 15)
                hits   = json.loads(r.read()).get('jobs', [])
                if not hits: break
                normed = [j for h in hits if (j := norm_himalayas(h))]
                cat_jobs.extend(normed)
                if len(hits) < 20: break   # last page
            except Exception as e:
                print(f"\n    ✗ Himalayas [{cat}] offset={offset}: {e}"); break
            time.sleep(0.2)
        jobs_all.extend(cat_jobs)
        print(f" {len(cat_jobs)}")
    return jobs_all

# ── 9. APIFY (OPTIONAL — costs money, skip if not needed) ────────────────────

def run_apify_actor(actor_id, input_data, timeout=120):
    """Run an Apify actor synchronously and return dataset items as a list.
    actor_id must use ~ separator: 'username~actor-name'
    """
    if not APIFY_TOKEN:
        return []
    # Apify REST API requires ~ not / in actor IDs
    safe_id = actor_id.replace('/', '~')
    url = (f"https://api.apify.com/v2/acts/{safe_id}/run-sync-get-dataset-items"
           f"?token={APIFY_TOKEN}&timeout={timeout}&memory=256")
    body = json.dumps(input_data).encode('utf-8')
    try:
        req = urllib.request.Request(url, data=body,
              headers={'Content-Type': 'application/json'}, method='POST')
        r = _urlopen(req, timeout + 30)
        return json.loads(r.read())
    except Exception as e:
        print(f"    ✗ Apify [{actor_id}]: {e}")
        return []

def _parse_days_ago(s):
    """Parse '3 days ago', '1 week ago', '2 months ago' → int days."""
    if not s: return 0
    s = s.lower().strip()
    try:
        n = int(s.split()[0])
        if 'month' in s: return n * 30
        if 'week'  in s: return n * 7
        if 'hour'  in s: return 0
        return n  # days
    except: return 0

def _guess_country(location_str):
    """Return 2-letter country code from a free-text location string."""
    loc = (location_str or '').lower()
    if 'dubai' in loc or 'abu dhabi' in loc or ' uae' in loc: return 'AE'
    if 'saudi' in loc or 'riyadh' in loc or 'jeddah' in loc: return 'SA'
    if 'qatar' in loc or 'doha' in loc: return 'QA'
    if 'kuwait' in loc: return 'KW'
    if 'bahrain' in loc or 'manama' in loc: return 'BH'
    if 'oman' in loc or 'muscat' in loc: return 'OM'
    if 'singapore' in loc: return 'sg'
    if 'hong kong' in loc: return 'HK'
    if 'london' in loc or ' uk' in loc or 'united kingdom' in loc: return 'gb'
    if 'germany' in loc or 'berlin' in loc or 'munich' in loc or 'frankfurt' in loc: return 'de'
    if 'switzerland' in loc or 'zurich' in loc or 'geneva' in loc: return 'ch'
    if 'netherlands' in loc or 'amsterdam' in loc or 'rotterdam' in loc: return 'nl'
    if 'ireland' in loc or 'dublin' in loc: return 'ie'
    if 'sweden' in loc or 'stockholm' in loc: return 'se'
    if 'spain' in loc or 'madrid' in loc or 'barcelona' in loc: return 'es'
    if 'italy' in loc or 'milan' in loc or 'rome' in loc: return 'it'
    if 'austria' in loc or 'vienna' in loc: return 'at'
    if 'france' in loc or 'paris' in loc: return 'fr'
    if 'india' in loc or 'mumbai' in loc or 'bangalore' in loc or 'bengaluru' in loc or 'delhi' in loc: return 'in'
    if 'canada' in loc or 'toronto' in loc or 'vancouver' in loc: return 'ca'
    if 'australia' in loc or 'sydney' in loc or 'melbourne' in loc: return 'au'
    if 'new york' in loc or 'chicago' in loc or 'san francisco' in loc or 'seattle' in loc: return 'us'
    return ''

def _extract_salary(sal_raw, cc):
    """Parse a free-text salary string into (sal_loc, sal_inr, sal_usd)."""
    if not sal_raw or not sal_raw.strip():
        return 'Not disclosed', 'Not disclosed', 0
    nums = [int(x.replace(',', '')) for x in re.findall(r'\d[\d,]+', sal_raw)]
    if not nums:
        return sal_raw, 'Not disclosed', 0
    lo, hi = (nums[0], nums[-1]) if len(nums) > 1 else (nums[0], nums[0])
    return fmt_salary(lo, hi, cc)

def norm_linkedin(hit, fallback_cc='AE'):
    """Normalise valig/linkedin-jobs-scraper output to standard job dict.
    Output fields: id, url, title, location, companyName, companyUrl,
    salary, postedDate, workType, contractType, experienceLevel,
    description, descriptionHtml, sector, applicationsCount
    """
    title   = (hit.get('title') or 'Analytics Role').strip()
    co      = (hit.get('companyName') or 'Unknown').strip()
    loc     = (hit.get('location') or '').strip()
    desc    = strip_html((hit.get('description') or hit.get('descriptionHtml') or '').strip())
    job_url = (hit.get('url') or '').strip()          # direct LinkedIn job posting URL
    co_url  = (hit.get('companyUrl') or '').strip()   # company LinkedIn page
    wm_raw  = (hit.get('workType') or '').strip()     # "Remote" / "On-site" / "Hybrid"
    pub     = (hit.get('postedDate') or '')
    sal_raw = (hit.get('salary') or '')

    # Build a reliable apply URL: prefer direct job link, else Google Jobs search
    if job_url and ('linkedin.com/jobs/view' in job_url or
                    ('linkedin.com/jobs' in job_url and 'search' not in job_url)):
        url = job_url
    elif job_url and job_url.startswith('http') and not re.search(
            r'/(careers|jobs|work-with-us|join-us|vacancies)/?$', job_url, re.I) \
            and len(job_url.rstrip('/').split('/')) > 3:
        url = job_url   # specific URL (has a path beyond just the domain)
    else:
        # Fall back to Google Jobs search — returns real current openings from all boards
        clean_title = re.sub(r'[–—&]', ' ', title)
        clean_title = re.sub(r'\s{2,}', ' ', clean_title).strip()
        city_hint   = loc.split(',')[0].strip() if loc else ''
        q = urllib.parse.quote_plus(f'{clean_title} {co} {city_hint}'.strip())
        url = f'https://www.google.com/search?q={q}&ibp=htl;jobs'

    if is_spam(title, desc, co): return None

    cc    = _guess_country(loc) or fallback_cc
    cname = COUNTRY_NAME.get(cc, cc.upper())
    city  = loc.split(',')[0].strip() if loc else cname

    sal_loc, sal_inr, sal_usd = _extract_salary(sal_raw, cc)

    pda = 0
    if pub:
        pda = _parse_days_ago(pub) if 'ago' in pub.lower() else days_ago_from_iso(pub)

    wm    = infer_work_mode(title, desc, 'remote' in wm_raw.lower())
    li_co = co.lower().replace(' ', '-').replace('.', '').replace(',', '')
    exp_label, exp_min = infer_experience(title, desc)
    return {
        'id': f"li_{abs(hash(url+title+co))}", 'title': title, 'position_name': title, 'company': co,
        'company_website': co_url or url, 'company_address': loc,
        'location': loc, 'city': city, 'country': cname,
        'salary_local': sal_loc, 'salary_inr_annual': sal_inr, 'salary_usd_annual': sal_usd,
        'posted_date': '', 'posted_days_ago': pda,
        'job_type': 'Full-time', 'experience_required': exp_label, 'experience_min': exp_min,
        'description': desc[:1000], 'responsibilities': [],
        'skills_required': [s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have': [],
        'hr_contact': {'name': f'{co} Recruiter', 'title': 'Talent Acquisition', 'email': '',
                       'linkedin': f'https://www.linkedin.com/company/{li_co}/jobs/', 'phone': extract_phone(desc)},
        'has_phone': bool(extract_phone(desc)),
        'is_mnc': True, 'company_size': 'Not disclosed', 'company_size_category': 'Unknown',
        'industry': 'Analytics', 'category': infer_category(title),
        'work_mode': wm, 'job_stability': 4.2,
        'glassdoor_rating': 4.2, 'glassdoor_reviews': 0,
        'apply_url': url, 'source': 'LinkedIn (Apify)', 'tags': [cname, 'LinkedIn'],
        'skills_match': [s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score': calc_fit(desc, title),
    }

# ── JobSpy blue collar searches ──────────────────────────────────────────────
JOBSPY_BC_SEARCHES = [
    # UAE / Dubai
    ('electrician job', 'Dubai',      'united arab emirates'),
    ('plumber job',     'Dubai',      'united arab emirates'),
    ('driver job',      'Dubai',      'united arab emirates'),
    ('ac technician',   'Dubai',      'united arab emirates'),
    ('carpenter job',   'Dubai',      'united arab emirates'),
    ('welder job',      'Dubai',      'united arab emirates'),
    ('hvac technician', 'Dubai',      'united arab emirates'),
    ('mechanic job',    'Dubai',      'united arab emirates'),
    ('maintenance technician','Dubai','united arab emirates'),
    ('tailor job',      'Dubai',      'united arab emirates'),
    ('mason job',       'Dubai',      'united arab emirates'),
    # UAE Abu Dhabi
    ('electrician job', 'Abu Dhabi',  'united arab emirates'),
    ('driver job',      'Abu Dhabi',  'united arab emirates'),
    ('plumber job',     'Abu Dhabi',  'united arab emirates'),
    ('ac technician',   'Abu Dhabi',  'united arab emirates'),
    ('mechanic job',    'Abu Dhabi',  'united arab emirates'),
    # Saudi Arabia
    ('electrician job', 'Riyadh',     'saudi arabia'),
    ('driver job',      'Riyadh',     'saudi arabia'),
    ('plumber job',     'Riyadh',     'saudi arabia'),
    ('ac technician',   'Riyadh',     'saudi arabia'),
    ('welder job',      'Riyadh',     'saudi arabia'),
    ('carpenter job',   'Riyadh',     'saudi arabia'),
    ('electrician job', 'Jeddah',     'saudi arabia'),
    ('driver job',      'Jeddah',     'saudi arabia'),
    ('maintenance technician','Jeddah','saudi arabia'),
    # Qatar
    ('electrician job', 'Doha',       'qatar'),
    ('driver job',      'Doha',       'qatar'),
    ('plumber job',     'Doha',       'qatar'),
    ('welder job',      'Doha',       'qatar'),
    ('ac technician',   'Doha',       'qatar'),
    ('carpenter job',   'Doha',       'qatar'),
    # Kuwait
    ('electrician job', 'Kuwait City','kuwait'),
    ('driver job',      'Kuwait City','kuwait'),
    ('plumber job',     'Kuwait City','kuwait'),
    ('maintenance technician','Kuwait City','kuwait'),
    # Oman
    ('electrician job', 'Muscat',     'oman'),
    ('driver job',      'Muscat',     'oman'),
    ('plumber job',     'Muscat',     'oman'),
    # Bahrain
    ('electrician job', 'Manama',     'bahrain'),
    ('driver job',      'Manama',     'bahrain'),
    ('maintenance technician','Manama','bahrain'),
    # India — Mumbai
    ('electrician job', 'Mumbai',     'india'),
    ('driver job',      'Mumbai',     'india'),
    ('plumber job',     'Mumbai',     'india'),
    ('ac technician',   'Mumbai',     'india'),
    ('welder job',      'Mumbai',     'india'),
    ('mechanic job',    'Mumbai',     'india'),
    # India — Delhi
    ('electrician job', 'Delhi',      'india'),
    ('driver job',      'Delhi',      'india'),
    ('plumber job',     'Delhi',      'india'),
    ('welder job',      'Delhi',      'india'),
    ('carpenter job',   'Delhi',      'india'),
    ('mechanic job',    'Delhi',      'india'),
    # India — Bangalore
    ('electrician job', 'Bangalore',  'india'),
    ('driver job',      'Bangalore',  'india'),
    ('welder job',      'Bangalore',  'india'),
    ('maintenance technician','Bangalore','india'),
    # India — Chennai
    ('electrician job', 'Chennai',    'india'),
    ('driver job',      'Chennai',    'india'),
    ('welder job',      'Chennai',    'india'),
    ('tailor job',      'Chennai',    'india'),
    # India — Hyderabad
    ('electrician job', 'Hyderabad',  'india'),
    ('driver job',      'Hyderabad',  'india'),
    ('ac technician',   'Hyderabad',  'india'),
    # India — Pune / Ahmedabad / Surat
    ('electrician job', 'Pune',       'india'),
    ('driver job',      'Pune',       'india'),
    ('tailor job',      'Ahmedabad',  'india'),
    ('cutting master',  'Ahmedabad',  'india'),
    ('electrician job', 'Ahmedabad',  'india'),
    ('tailor job',      'Surat',      'india'),
    ('cutting master',  'Surat',      'india'),
    # India — General (catchall)
    ('driver job',      'India',      'india'),
    ('electrician job', 'India',      'india'),
    ('hvac technician', 'India',      'india'),
    ('carpenter job',   'India',      'india'),
    # UK blue collar
    ('electrician job', 'London',     'uk'),
    ('plumber job',     'London',     'uk'),
    ('hvac engineer',   'London',     'uk'),
    ('mechanic job',    'Birmingham', 'uk'),
    ('electrician job', 'Manchester', 'uk'),
    ('carpenter job',   'London',     'uk'),
    # Australia blue collar
    ('electrician job', 'Sydney',     'australia'),
    ('plumber job',     'Melbourne',  'australia'),
    ('welder job',      'Brisbane',   'australia'),
    ('hvac technician', 'Sydney',     'australia'),
    ('mechanic job',    'Perth',      'australia'),
    ('carpenter job',   'Melbourne',  'australia'),
]

CC_MAP_JOBSPY = {
    'united arab emirates': 'AE', 'saudi arabia': 'SA', 'qatar': 'QA',
    'kuwait': 'KW', 'oman': 'OM', 'bahrain': 'BH',
    'india': 'IN', 'uk': 'GB', 'united kingdom': 'GB',
    'australia': 'AU', 'canada': 'CA', 'germany': 'DE',
    'singapore': 'SG', 'usa': 'US',
}

def fetch_jobspy():
    """Scrape Indeed blue collar jobs via JobSpy (no API key, Python 3.11+)."""
    if not JOBSPY_OK:
        print("  Skipped (python-jobspy not installed — run: pip install python-jobspy)")
        return []

    import warnings
    warnings.filterwarnings('ignore')

    results = []
    seen_urls = set()
    for term, location, country in JOBSPY_BC_SEARCHES:
        print(f"    {term} / {location} …", end='', flush=True)
        try:
            df = _jobspy_scrape(
                site_name=['indeed'],
                search_term=term,
                location=location,
                results_wanted=50,
                hours_old=2160,       # 90 days
                country_indeed=country,
            )
            if df is None or len(df) == 0:
                print(" 0 jobs"); time.sleep(1.0); continue
            cc = CC_MAP_JOBSPY.get(country.lower(), 'UN')
            cname = COUNTRY_NAME.get(cc) or country.title()
            count = 0
            for _, row in df.iterrows():
                url = str(row.get('job_url') or '')
                if url in seen_urls: continue
                seen_urls.add(url)
                title  = str(row.get('title')   or '').strip()
                co     = str(row.get('company') or '').strip() or 'Unknown'
                desc   = str(row.get('description') or '').strip()
                loc    = str(row.get('location')    or location).strip()
                city   = loc.split(',')[0].strip() if ',' in loc else location
                posted = str(row.get('date_posted') or '')[:10]

                if is_spam(title, desc, co): continue
                if not title or co == 'Unknown': continue

                sal_min = float(row.get('min_amount') or 0)
                sal_max = float(row.get('max_amount') or 0)
                cur_raw = str(row.get('currency') or '')
                cur_cc_map = {'AED':'AE','SAR':'SA','INR':'in','GBP':'gb','AUD':'au','USD':'USD','EUR':'de'}
                sal_loc, sal_inr, sal_usd = fmt_salary(sal_min, sal_max, cur_cc_map.get(cur_raw, cc))

                exp_label, exp_min_yr = infer_experience(title, desc)
                phone = extract_phone(desc)
                li_co = co.lower().replace(' ','-').replace('.','').replace(',','')

                j = {
                    'id': f"jspy_{abs(hash(url+title))}",
                    'title': title, 'position_name': title, 'company': co,
                    'company_website': str(row.get('company_url') or ''),
                    'company_address': loc, 'location': loc,
                    'city': city, 'country': cname,
                    'salary_local': sal_loc, 'salary_inr_annual': sal_inr, 'salary_usd_annual': sal_usd,
                    'posted_date': posted, 'posted_days_ago': days_ago_from_iso(posted+'T00:00:00') if posted else 0,
                    'job_type': str(row.get('job_type') or 'Full-time').replace('fulltime','Full-time'),
                    'experience_required': exp_label, 'experience_min': exp_min_yr,
                    'description': desc[:1000], 'responsibilities': [],
                    'skills_required': [s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
                    'nice_to_have': [],
                    'hr_contact': {'name': f'{co} HR', 'title': 'Talent Acquisition',
                                   'email': '', 'phone': phone,
                                   'linkedin': f'https://www.linkedin.com/company/{li_co}/jobs/'},
                    'has_phone': bool(phone),
                    'is_mnc': False, 'company_size': 'Not disclosed', 'company_size_category': 'Unknown',
                    'industry': 'Trades', 'category': infer_category(title),
                    'work_mode': infer_work_mode(title, desc, bool(row.get('is_remote'))),
                    'job_stability': 4.0, 'glassdoor_rating': 0.0, 'glassdoor_reviews': 0,
                    'apply_url': url, 'source': 'JobSpy/Indeed',
                    'tags': [cname, 'Blue Collar'],
                    'skills_match': [s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
                    'fit_score': calc_fit(desc, title),
                }
                results.append(j); count += 1
            print(f" {count} jobs")
        except Exception as e:
            print(f" ✗ {e}")
        time.sleep(1.5)  # be polite to Indeed

    return results

def fetch_apify_linkedin(queries_cc):
    """Fetch jobs via valig/linkedin-jobs-scraper Apify actor."""
    jobs = []
    for keywords, location, cc in queries_cc:
        print(f"    {keywords} / {location} …", end='', flush=True)
        items = run_apify_actor('valig/linkedin-jobs-scraper', {
            'keywords':   keywords,
            'location':   location,
            'limit':      25,
            'datePosted': 'r2592000',   # last 30 days (LinkedIn format)
        }, timeout=90)
        normed = [j for h in items if (j := norm_linkedin(h, cc))]
        jobs.extend(normed)
        print(f" {len(normed)} jobs")
        time.sleep(1.0)
    return jobs

def norm_google_jobs(hit, fallback_cc='us'):
    """Normalise orgupdate/google-jobs-scraper output to standard job dict.
    Actual output fields: job_title, company_name, location, posted_via,
    salary, date, URL, description
    """
    title   = (hit.get('job_title') or hit.get('title') or 'Analytics Role').strip()
    co      = (hit.get('company_name') or hit.get('companyName') or 'Unknown').strip()
    loc     = (hit.get('location') or '').strip()
    desc    = (hit.get('description') or '').strip()
    pub     = hit.get('date') or ''
    sal_raw = hit.get('salary') or ''
    wfh     = 'remote' in loc.lower() or 'remote' in desc[:200].lower()
    url     = hit.get('URL') or hit.get('url') or ''

    if is_spam(title, desc, co): return None

    cc    = _guess_country(loc) or fallback_cc
    cname = COUNTRY_NAME.get(cc, cc.upper())
    city  = loc.split(',')[0].strip() if loc else cname

    sal_loc, sal_inr, sal_usd = _extract_salary(sal_raw, cc)
    pda  = _parse_days_ago(pub) if pub else 0
    wm   = infer_work_mode(title, desc, wfh)
    li_co = co.lower().replace(' ', '-').replace('.', '').replace(',', '')
    exp_label, exp_min = infer_experience(title, desc)
    return {
        'id': f"gj_{abs(hash(url+title+co))}", 'title': title, 'position_name': title, 'company': co,
        'company_website': url, 'company_address': loc,
        'location': loc, 'city': city, 'country': cname,
        'salary_local': sal_loc, 'salary_inr_annual': sal_inr, 'salary_usd_annual': sal_usd,
        'posted_date': '', 'posted_days_ago': pda,
        'job_type': 'Full-time', 'experience_required': exp_label, 'experience_min': exp_min,
        'description': desc[:1000], 'responsibilities': [],
        'skills_required': [s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have': [],
        'hr_contact': {'name': f'{co} Recruiter', 'title': 'Talent Acquisition', 'email': '',
                       'linkedin': f'https://www.linkedin.com/company/{li_co}/jobs/', 'phone': extract_phone(desc)},
        'has_phone': bool(extract_phone(desc)),
        'is_mnc': True, 'company_size': 'Not disclosed', 'company_size_category': 'Unknown',
        'industry': 'Analytics', 'category': infer_category(title),
        'work_mode': wm, 'job_stability': 4.0,
        'glassdoor_rating': 4.0, 'glassdoor_reviews': 0,
        'apply_url': url, 'source': 'Google Jobs (Apify)', 'tags': [cname, 'Google Jobs'],
        'skills_match': [s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score': calc_fit(desc, title),
    }

def fetch_apify_google_jobs(queries_cc):
    """Fetch jobs via orgupdate/google-jobs-scraper Apify actor."""
    jobs = []
    for keywords, location, cc in queries_cc:
        query = f"{keywords} {location}"
        print(f"    {query} …", end='', flush=True)
        items = run_apify_actor('orgupdate/google-jobs-scraper', {
            'query':    query,
            'maxItems': 20,
        }, timeout=90)
        normed = [j for h in items if (j := norm_google_jobs(h, cc))]
        jobs.extend(normed)
        print(f" {len(normed)} jobs")
        time.sleep(1.0)
    return jobs

# ── DEDUP ─────────────────────────────────────────────────────────────────────
def dedup(jobs):
    seen = {}
    for j in jobs:
        key = ((j.get('title') or '').strip().lower(),(j.get('company') or '').strip().lower())
        ex = seen.get(key)
        if ex is None or (j.get('posted_days_ago',999) < ex.get('posted_days_ago',999)):
            seen[key] = j
    return list(seen.values())

# ── MAIN ──────────────────────────────────────────────────────────────────────
def fetch_all_jobs():
    all_jobs, seen_ids = [], set()
    def add(jobs):
        for j in jobs:
            if j and j.get('id') not in seen_ids:
                seen_ids.add(j['id']); all_jobs.append(j)

    print("\n── Adzuna ───────────────────────────────────────────────────────")
    prev = None
    for cc, query, display in ADZUNA_SEARCHES:
        if display != prev: print(f"  [{display}]"); prev = display
        city_hint = display.split(' - ',1)[1] if ' - ' in display else None
        print(f"    {query} …", end='', flush=True)
        results = fetch_adzuna(cc, query, city_hint=city_hint)
        add(results); print(f" {len(results)} jobs"); time.sleep(0.4)

    print("\n── Reed.co.uk (UK) ──────────────────────────────────────────────")
    for keywords, location in REED_SEARCHES:
        print(f"    {keywords} / {location} …", end='', flush=True)
        results = fetch_reed(keywords, location)
        add(results); print(f" {len(results)} jobs"); time.sleep(0.6)

    print("\n── JSearch (Dubai / Gulf) ───────────────────────────────────────")
    if jsearch_already_ran_today():
        print("  Skipped (already ran today — quota management)")
    else:
        js_total = 0; js_consec_429 = 0
        for query, cc in JSEARCH_SEARCHES:
            if js_consec_429 >= 3:
                print("  ⚠ JSearch quota exhausted (3 consecutive 429s) — skipping remaining")
                break
            print(f"    {query} …", end='', flush=True)
            results = fetch_jsearch(query, cc)
            if results is None:  # 429 signal
                js_consec_429 += 1; print(f" 0 jobs (429)"); time.sleep(1.0); continue
            js_consec_429 = 0
            add(results); js_total += len(results); print(f" {len(results)} jobs"); time.sleep(1.0)
        if js_total > 0: mark_jsearch_ran(); print(f"  JSearch total: {js_total} | marked as ran today")

    print("\n── RemoteOK (free / remote) ─────────────────────────────────────")
    add(fetch_remoteok())

    print("\n── Arbeitnow (free / Europe) ────────────────────────────────────")
    add(fetch_arbeitnow())

    print("\n── Remotive (free / remote worldwide) ───────────────────────────")
    add(fetch_remotive())

    print("\n── Jobicy (free / remote worldwide) ─────────────────────────────")
    add(fetch_jobicy())

    print("\n── Himalayas (free / remote + salary) ───────────────────────────")
    add(fetch_himalayas())

    print("\n── JobSpy / Indeed (blue collar — Gulf + India + UK + AU) ──────")
    add(fetch_jobspy())

    if APIFY_TOKEN:
        print("\n── Apify – LinkedIn (optional / paid) ───────────────────────────")
        add(fetch_apify_linkedin(APIFY_LI_SEARCHES))

        print("\n── Apify – Google Jobs (optional / paid) ────────────────────────")
        add(fetch_apify_google_jobs(APIFY_GJ_SEARCHES))
    else:
        print("\n── Apify skipped (APIFY_TOKEN not set — not needed, all free sources active) ──")

    # Curated sample jobs intentionally excluded from live feed —
    # they are fictional showcase entries without real apply URLs.

    before = len(all_jobs)
    all_jobs = dedup(all_jobs)
    all_jobs.sort(key=lambda j: j.get('salary_usd_annual',0), reverse=True)

    from collections import Counter
    countries = Counter(j.get('country','?') for j in all_jobs)
    cats = Counter(j.get('category','?') for j in all_jobs)
    print(f"\n{'─'*55}")
    print(f"  Total: {len(all_jobs)} clean jobs  (removed {before-len(all_jobs)} dupes)")
    print(f"  By country: {dict(sorted(countries.items(),key=lambda x:-x[1]))}")
    print(f"  By category:{dict(sorted(cats.items(),key=lambda x:-x[1]))}")
    return all_jobs


# Also expose as get_jobs() for backward compat with old app.py imports
def get_jobs():
    path = os.path.join(BASE, 'data', 'live_jobs.json')
    fallback = os.path.join(BASE, 'data', 'sample_jobs.json')
    p = path if os.path.exists(path) else fallback
    with open(p) as f: return json.load(f)


if __name__ == '__main__':
    jobs = fetch_all_jobs()
    out = os.path.join(BASE, 'data', 'live_jobs.json')
    with open(out, 'w') as f: json.dump(jobs, f, indent=2)
    ts_path = os.path.join(BASE, 'data', 'last_updated.json')
    with open(ts_path, 'w') as f:
        apify_src = ' + Apify (LI+GJ)' if APIFY_TOKEN else ''
        json.dump({'timestamp':datetime.now(timezone.utc).isoformat(),'count':len(jobs),
                   'source':f'Adzuna + Reed + JSearch + RemoteOK + Arbeitnow + Remotive + Jobicy + Himalayas{apify_src}'}, f)
    print(f"\n  Saved → {out}")
