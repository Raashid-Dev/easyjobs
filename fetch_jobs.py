"""
fetch_jobs.py — EasyJobs multi-source job fetcher
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Sources:
  1. Adzuna  — USA, Switzerland, Germany, UK, India (Mumbai/Delhi/Bangalore),
               Singapore, Australia, Canada, France  (free, unlimited)
  2. Reed.co.uk — London / UK (free, 50 req/day)
  3. JSearch    — LinkedIn + Indeed globally incl. Dubai/Gulf
                  (free 200 req/month, runs once per day)
  4. Curated    — sample_jobs.json (Gulf hand-picked)
"""

import os, json, time, base64, urllib.request, urllib.parse
from datetime import datetime, timezone, date

BASE = os.path.dirname(os.path.abspath(__file__))

# ── API KEYS ──────────────────────────────────────────────────────────────────
ADZUNA_APP_ID  = os.getenv('ADZUNA_APP_ID',  '87bbd506')
ADZUNA_APP_KEY = os.getenv('ADZUNA_APP_KEY', 'fd79dfa64839c87f9bacae9dc3fde106')
REED_API_KEY   = os.getenv('REED_API_KEY',   '565ae00f-9e91-4e97-98a9-1bfa100d0ae1')
JSEARCH_KEY    = os.getenv('JSEARCH_KEY',    '4f20b35572msh47c8161a9c003d7p173df1jsn1bb4dc2fe02d')

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
]

REED_SEARCHES = [
    ('data analytics manager','London'),
    ('business intelligence manager','London'),
    ('digital analytics manager','London'),
    ('marketing analytics','London'),
    ('web analytics manager','London'),
    ('data analytics manager','Manchester'),
]

JSEARCH_SEARCHES = [
    ('data analytics manager in Dubai UAE','AE'),
    ('digital marketing analytics manager Dubai','AE'),
    ('business intelligence manager Dubai UAE','AE'),
    ('data analytics manager Riyadh Saudi Arabia','SA'),
    ('senior analytics manager Abu Dhabi UAE','AE'),
    ('digital analytics lead Dubai UAE','AE'),
]
JSEARCH_DAILY_FILE = os.path.join(BASE, 'data', 'jsearch_last_run.json')

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
    'analytic','analytics','data ','data-',' data','intelligence',' bi ',
    'dashboard','reporting','report','insight','kpi','digital marketing',
    'seo','sem','sql','power bi','tableau','looker','google analytics',
    'ga4','gtm','tag manager','marketing analytics','web analytics',
    'performance marketing','crm','visualization','bigquery','measurement',
    'tracking','attribution','conversion rate','cro','mis ','information system',
    'business analyst','growth analyst','growth manager','media analyst','performance analyst',
    'operations manager','revenue operations','marketing operations',
    'business operations','growth operations','operations analyst','ops manager','ops lead',
    'backend developer','backend engineer','full stack developer','fullstack developer',
    'data engineer','analytics engineer','software developer','python developer',
    'node.js developer','api developer','platform engineer','engineering manager',
]
HARD_BLOCK = [
    'driver','delivery boy','warehouse','nurse','teacher','security guard',
    'cook ','chef','cashier','receptionist','electrician','plumber','mechanic',
    'telemarketing','domestic','factory worker','labourer','content writer',
    'copywriter','solicitor','lawyer','legal counsel','construction manager',
    'pre construction','quantity surveyor','customer engineer',
    'finance business partner','management accounting','market risk controller',
    'java developer','kotlin developer','cloud engineer','sap ewm','sap bpa',
    'generative ai engineer','campus','associate (ca)','content creator',
]
SPAM_COMPANIES = ['testhiring','flat fee recruiter','wynwood tech']
DESC_KW = [
    'google analytics','ga4','power bi','tableau','looker','bigquery','sql',
    'dashboard','kpi','analytics','data analysis','reporting','digital marketing',
    'seo','tracking','attribution','conversion','insight','metric','measurement',
    'operations','backend','data engineer','revenue operations','python','api',
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
def calc_fit(text):
    t = (text or '').lower()
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
    'AE':('AED',23,0.272),'ae':('AED',23,0.272),
    'SA':('SAR',22,0.267),'sa':('SAR',22,0.267),
    'BH':('BHD',222,2.65),'bh':('BHD',222,2.65),
}
COUNTRY_NAME = {
    'ch':'Switzerland','CH':'Switzerland','de':'Germany','DE':'Germany',
    'gb':'UK','GB':'UK','in':'India','IN':'India','sg':'Singapore','SG':'Singapore',
    'au':'Australia','AU':'Australia','ca':'Canada','CA':'Canada',
    'fr':'France','FR':'France','AE':'UAE','ae':'UAE',
    'SA':'Saudi Arabia','sa':'Saudi Arabia','BH':'Bahrain','bh':'Bahrain',
    'US':'USA','us':'USA',
}

def fmt_salary(lo, hi, cc):
    sym, inr_rate, usd_rate = CURRENCY.get(cc, ('USD',83,1.0))
    mid = (lo + hi) / 2 if hi > lo else lo
    sal_inr = f"{mid * inr_rate / 1e7:.2f} CR" if mid else 'Not disclosed'
    sal_loc = f"{sym} {int(lo):,} – {int(hi):,} / year" if hi > 0 else 'Not disclosed'
    sal_usd = int(mid * usd_rate) if mid else 0
    return sal_loc, sal_inr, sal_usd

def infer_category(title):
    t = title.lower()
    if any(k in t for k in ['operations manager','revenue operations','marketing operations','business operations','ops manager','ops lead']):
        return 'Operations'
    if any(k in t for k in ['backend developer','backend engineer','full stack','fullstack','data engineer','analytics engineer','software developer','python developer','node.js','api developer','engineering manager']):
        return 'Backend'
    if any(k in t for k in ['marketing','seo','sem','campaign','social media','paid']):
        return 'Digital Marketing'
    if any(k in t for k in [' bi ',' bi,','business intel','business intelligence']):
        return 'Business Intelligence'
    if any(k in t for k in ['web analytics','tag manager','gtm']):
        return 'Web Analytics'
    return 'Data Analytics'

def infer_work_mode(title, desc, is_remote=False):
    t = (title + ' ' + (desc or ''))[:500].lower()
    if is_remote or 'remote' in t or 'work from home' in t: return 'WFH'
    if 'hybrid' in t: return 'Hybrid'
    if 'freelance' in t or 'contract' in t: return 'Freelance'
    return 'Hybrid'

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
    return {
        'id':f"az_{hit.get('id','')}",'title':title,'position_name':title,'company':co,
        'company_website':hit.get('redirect_url',''),'company_address':loc,
        'location':loc,'city':area or loc,'country':cname,
        'salary_local':sal_loc,'salary_inr_annual':sal_inr,'salary_usd_annual':sal_usd,
        'posted_date':hit.get('created','')[:10],
        'posted_days_ago':days_ago_from_iso(hit.get('created','')),
        'job_type':'Full-time','experience_required':'5+ years','experience_min':5,
        'description':desc,'responsibilities':[],
        'skills_required':[s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have':[],
        'hr_contact':{'name':f'{co} HR','title':'Talent Acquisition','email':'','linkedin':f'https://www.linkedin.com/company/{li_co}/jobs/','phone':''},
        'is_mnc':True,'company_size':'Not disclosed','company_size_category':'Unknown',
        'industry':cat,'category':infer_category(title),
        'work_mode':infer_work_mode(title,desc),'job_stability':4.0,
        'glassdoor_rating':4.0,'glassdoor_reviews':0,
        'apply_url':hit.get('redirect_url',''),'source':'Adzuna','tags':[cat,cname],
        'skills_match':[s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score':calc_fit(desc),
    }

def fetch_adzuna(cc, query, city_hint=None):
    params = urllib.parse.urlencode({'app_id':ADZUNA_APP_ID,'app_key':ADZUNA_APP_KEY,'results_per_page':20,'what':query,'sort_by':'date'})
    url = f"{ADZUNA_URL.format(country=cc)}?{params}"
    try:
        r = urllib.request.urlopen(url, timeout=15)
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
    loc = hit.get('locationName','UK'); desc = hit.get('jobDescription','')
    if is_spam(title, desc, co): return None
    li_co = co.lower().replace(' ','-').replace('.','').replace(',','')
    return {
        'id':f"rd_{hit.get('jobId','')}",'title':title,'position_name':title,'company':co,
        'company_website':hit.get('jobUrl',''),'company_address':loc,
        'location':loc,'city':loc,'country':'UK',
        'salary_local':sal_loc,'salary_inr_annual':sal_inr,'salary_usd_annual':sal_usd,
        'posted_date':(hit.get('date','') or '')[:10],
        'posted_days_ago':days_ago_from_dmy(hit.get('date','')) if hit.get('date') else 0,
        'job_type':'Full-time','experience_required':'5+ years','experience_min':5,
        'description':desc,'responsibilities':[],
        'skills_required':[s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have':[],
        'hr_contact':{'name':f'{co} Talent Team','title':'Talent Acquisition','email':'','linkedin':f'https://www.linkedin.com/company/{li_co}/jobs/','phone':''},
        'is_mnc':False,'company_size':'Not disclosed','company_size_category':'Unknown',
        'industry':'Analytics','category':infer_category(title),
        'work_mode':infer_work_mode(title,desc),'job_stability':3.8,
        'glassdoor_rating':3.8,'glassdoor_reviews':0,
        'apply_url':hit.get('jobUrl',''),'source':'Reed.co.uk','tags':['UK','London','Analytics'],
        'skills_match':[s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score':calc_fit(desc),
    }

def fetch_reed(keywords, location):
    creds = base64.b64encode(f"{REED_API_KEY}:".encode()).decode()
    params = urllib.parse.urlencode({'keywords':keywords,'locationName':location,'resultsToTake':50,'fullTime':'true'})
    try:
        req = urllib.request.Request(f"{REED_URL}?{params}", headers={'Authorization':f'Basic {creds}'})
        r = urllib.request.urlopen(req, timeout=15)
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
    title = hit.get('job_title','Analytics Role'); co = hit.get('employer_name','Unknown')
    desc = hit.get('job_description',''); city = hit.get('job_city','')
    cc = hit.get('job_country','AE'); url = hit.get('job_apply_link','')
    pub = hit.get('job_publisher','Indeed'); ts = hit.get('job_posted_at_timestamp',0)
    remote = hit.get('job_is_remote',False)
    lo = hit.get('job_min_salary') or 0; hi = hit.get('job_max_salary') or 0
    per = hit.get('job_salary_period') or 'YEAR'; cur = hit.get('job_salary_currency') or ''
    if per == 'MONTH': lo,hi = lo*12,hi*12
    if per == 'HOUR':  lo,hi = lo*2080,hi*2080
    cur_map = {'AED':'AE','SAR':'SA','BHD':'BH','SGD':'sg','GBP':'gb','EUR':'de','USD':'USD','INR':'in','AUD':'au','CAD':'ca'}
    sal_loc, sal_inr, sal_usd = fmt_salary(lo, hi, cur_map.get(cur, cc))
    if is_spam(title, desc, co): return None
    cname = COUNTRY_NAME.get(cc) or COUNTRY_NAME.get(cc.lower()) or COUNTRY_NAME.get(cc.upper()) or cc
    if not cname or len(cname) <= 2: cname = 'Unknown'
    li_co = co.lower().replace(' ','-').replace('.','').replace(',','')
    return {
        'id':f"js_{abs(hash(url+title))}",'title':title,'position_name':title,'company':co,
        'company_website':hit.get('employer_website',''),'company_address':f"{city}, {cname}",
        'location':f"{city}, {cname}",'city':city or cname,'country':cname,
        'salary_local':sal_loc,'salary_inr_annual':sal_inr,'salary_usd_annual':sal_usd,
        'posted_date':datetime.fromtimestamp(ts).strftime('%Y-%m-%d') if ts else '',
        'posted_days_ago':days_ago_from_ts(ts) if ts else 0,
        'job_type':'Full-time','experience_required':'5+ years','experience_min':5,
        'description':desc[:1000],'responsibilities':hit.get('job_highlights',{}).get('Responsibilities',[])[:6],
        'skills_required':(hit.get('job_required_skills') or [])[:8] or [s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have':[],
        'hr_contact':{'name':f'{co} Recruiter','title':'Talent Acquisition','email':'','linkedin':f'https://www.linkedin.com/company/{li_co}/jobs/','phone':''},
        'is_mnc':True,'company_size':'Not disclosed','company_size_category':'Unknown',
        'industry':'Analytics','category':infer_category(title),
        'work_mode':infer_work_mode(title,desc,remote),'job_stability':4.0,
        'glassdoor_rating':4.0,'glassdoor_reviews':0,
        'apply_url':url,'source':f'JSearch ({pub})','tags':[cname,'Analytics'],
        'skills_match':[s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score':calc_fit(desc),
    }

def fetch_jsearch(query, cc):
    params = urllib.parse.urlencode({'query':query,'page':'1','num_pages':'1','date_posted':'month'})
    try:
        req = urllib.request.Request(f"{JSEARCH_URL}?{params}", headers={
            'x-rapidapi-host':'jsearch.p.rapidapi.com','x-rapidapi-key':JSEARCH_KEY,'Content-Type':'application/json'})
        r = urllib.request.urlopen(req, timeout=15)
        d = json.loads(r.read())
        if d.get('status') != 'OK': print(f"    ✗ JSearch '{query}': {d.get('status')}"); return []
        return [j for h in d.get('data',[]) if (j := norm_jsearch(h))]
    except Exception as e:
        err = str(e)
        if '403' in err: print(f"    ✗ JSearch: API key not subscribed")
        else: print(f"    ✗ JSearch '{query}': {e}")
        return []

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
        js_total = 0
        for query, cc in JSEARCH_SEARCHES:
            print(f"    {query} …", end='', flush=True)
            results = fetch_jsearch(query, cc)
            add(results); js_total += len(results); print(f" {len(results)} jobs"); time.sleep(1.0)
        if js_total > 0: mark_jsearch_ran(); print(f"  JSearch total: {js_total} | marked as ran today")

    print("\n── Curated sample ───────────────────────────────────────────────")
    sample_path = os.path.join(BASE, 'data', 'sample_jobs.json')
    added = 0
    if os.path.exists(sample_path):
        with open(sample_path) as f:
            for j in json.load(f):
                if j.get('id') not in seen_ids and not is_spam(j.get('title',''), j.get('description',''), j.get('company','')):
                    seen_ids.add(j['id']); all_jobs.append(j); added += 1
    print(f"  Added {added} curated jobs")

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
        json.dump({'timestamp':datetime.now(timezone.utc).isoformat(),'count':len(jobs),'source':'Adzuna + Reed + JSearch + Curated'}, f)
    print(f"\n  Saved → {out}")
