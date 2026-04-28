"""
fetch_jobs.py
  - Switzerland jobs  → Adzuna API (your key)
  - Dubai / UAE jobs  → CareerJet public API (no key needed)
  - Fallback          → curated sample_jobs.json
"""
import os, json, time, requests
from datetime import datetime, timezone

# ── Adzuna (Switzerland) ──────────────────────────────────────────────────────
ADZUNA_APP_ID  = os.getenv('ADZUNA_APP_ID',  '87bbd506')
ADZUNA_APP_KEY = os.getenv('ADZUNA_APP_KEY', 'fd79dfa64839c87f9bacae9dc3fde106')
ADZUNA_URL     = 'https://api.adzuna.com/v1/api/jobs/{country}/search/1'

ADZUNA_SEARCHES = [
    ('ch', 'data analytics manager'),
    ('ch', 'business intelligence manager'),
    ('ch', 'digital analytics lead'),
    ('ch', 'senior data analyst'),
    ('ch', 'power bi analytics'),
    ('ch', 'web analytics'),
]

# ── CareerJet (Dubai / UAE) ───────────────────────────────────────────────────
CAREERJET_URL = 'http://public.api.careerjet.net/search'
# Dubai: no free public API available for UAE without registration.
# Dubai jobs come from curated sample_jobs.json (real companies, realistic salaries).
# To add more Dubai jobs manually, edit data/sample_jobs.json.

# ── Profile for fit scoring ───────────────────────────────────────────────────
MY_SKILLS = [
    'ga4', 'google analytics', 'gtm', 'google tag manager',
    'sql', 'bigquery', 'power bi', 'looker', 'looker studio', 'tableau',
    'data visualization', 'analytics', 'business intelligence',
    'digital analytics', 'web analytics', 'kpi', 'dashboard', 'reporting',
    'retail analytics', 'campaign analytics', 'dax', 'a/b testing',
    'crm analytics', 'rfm', 'clv', 'marketing analytics'
]
MY_EXP_YEARS      = 10
TARGET_SALARY_USD = 180000

# ── Helpers ───────────────────────────────────────────────────────────────────
def calc_fit(text, extra_skills=None):
    combined = (text or '').lower() + ' ' + ' '.join(extra_skills or []).lower()
    matched  = sum(1 for s in MY_SKILLS if s in combined)
    skill_pct = min(100, int((matched / len(MY_SKILLS)) * 220))
    exp_match = 100 if MY_EXP_YEARS >= 8 else 70
    return min(98, max(40, int(skill_pct * 0.7 + exp_match * 0.3)))

def days_ago(date_str):
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return max(0, (datetime.now(timezone.utc) - dt).days)
    except Exception:
        return 0

def chf_to_inr_cr(chf):
    return f"{chf * 95 / 1e7:.2f} CR" if chf else 'Not disclosed'

def aed_to_inr_cr(aed):
    return f"{aed * 23 / 1e7:.2f} CR" if aed else 'Not disclosed'

# ── Adzuna normaliser ─────────────────────────────────────────────────────────
def norm_adzuna(hit):
    lo  = hit.get('salary_min', 0) or 0
    hi  = hit.get('salary_max', 0) or 0
    mid = (lo + hi) / 2 if hi > 0 else lo

    sal_inr  = chf_to_inr_cr(mid)
    sal_loc  = f"CHF {int(lo):,} – {int(hi):,} / year" if hi > 0 else 'Not disclosed'
    sal_usd  = int(mid * 1.12) if mid else 0

    desc = hit.get('description', '')
    cat  = hit.get('category', {}).get('label', 'Analytics')
    loc  = hit.get('location', {}).get('display_name', '')
    area = (hit.get('location', {}).get('area') or [''])[0]
    co   = hit.get('company', {}).get('display_name', 'Unknown')

    return {
        'id':                   f"az_{hit.get('id','')}",
        'title':                hit.get('title', 'Analytics Role'),
        'position_name':        hit.get('title', 'Analytics Role'),
        'company':              co,
        'company_website':      hit.get('redirect_url', ''),
        'company_address':      loc,
        'location':             loc,
        'city':                 area or loc,
        'country':              'Switzerland',
        'salary_local':         sal_loc,
        'salary_inr_annual':    sal_inr,
        'salary_usd_annual':    sal_usd,
        'posted_date':          hit.get('created', '')[:10],
        'posted_days_ago':      days_ago(hit.get('created', '')),
        'job_type':             'Full-time',
        'experience_required':  '5+ years',
        'experience_min':       5,
        'description':          desc,
        'responsibilities':     [],
        'skills_required':      [s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have':         [],
        'hr_contact':           {'name': f'{co} HR', 'title': 'Talent Acquisition',
                                 'email': '', 'linkedin': '', 'phone': ''},
        'is_mnc':               True,
        'company_size':         'Not disclosed',
        'company_size_category':'Unknown',
        'industry':             cat,
        'job_stability':        4.0,
        'glassdoor_rating':     4.0,
        'glassdoor_reviews':    0,
        'apply_url':            hit.get('redirect_url', ''),
        'source':               'Adzuna / jobs.ch',
        'tags':                 [cat, 'Switzerland'],
        'skills_match':         [s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score':            calc_fit(desc),
    }

# ── CareerJet normaliser ──────────────────────────────────────────────────────
def norm_careerjet(hit):
    desc     = hit.get('description', '')
    title    = hit.get('title', 'Analytics Role')
    co       = hit.get('company', 'Unknown')
    loc      = hit.get('locations', 'Dubai, UAE')
    url      = hit.get('url', '')
    date_str = hit.get('date', '')

    return {
        'id':                   f"cj_{abs(hash(url))}",
        'title':                title,
        'position_name':        title,
        'company':              co,
        'company_website':      url,
        'company_address':      loc,
        'location':             loc,
        'city':                 'Dubai',
        'country':              'UAE',
        'salary_local':         'Not disclosed',
        'salary_inr_annual':    'Not disclosed',
        'salary_usd_annual':    0,
        'posted_date':          date_str[:10] if date_str else '',
        'posted_days_ago':      days_ago(date_str) if date_str else 0,
        'job_type':             'Full-time',
        'experience_required':  '5+ years',
        'experience_min':       5,
        'description':          desc,
        'responsibilities':     [],
        'skills_required':      [s.upper() for s in MY_SKILLS if s in desc.lower()][:8],
        'nice_to_have':         [],
        'hr_contact':           {'name': f'{co} HR Team', 'title': 'Talent Acquisition',
                                 'email': '', 'linkedin': '', 'phone': ''},
        'is_mnc':               True,
        'company_size':         'Not disclosed',
        'company_size_category':'Unknown',
        'industry':             'Analytics',
        'job_stability':        3.8,
        'glassdoor_rating':     3.8,
        'glassdoor_reviews':    0,
        'apply_url':            url,
        'source':               'CareerJet / Bayt.com',
        'tags':                 ['Dubai', 'UAE', 'Analytics'],
        'skills_match':         [s.upper() for s in MY_SKILLS if s in desc.lower()][:6],
        'fit_score':            calc_fit(desc),
    }

# ── Fetchers ──────────────────────────────────────────────────────────────────
def fetch_adzuna(country, query):
    params = {
        'app_id':           ADZUNA_APP_ID,
        'app_key':          ADZUNA_APP_KEY,
        'results_per_page': 20,
        'what':             query,
        'sort_by':          'date',
        'content-type':     'application/json',
    }
    try:
        r = requests.get(ADZUNA_URL.format(country=country), params=params, timeout=15)
        r.raise_for_status()
        return [norm_adzuna(h) for h in r.json().get('results', [])]
    except Exception as e:
        print(f"  ✗ Adzuna [{country}] '{query}': {e}")
        return []

# ── Main ──────────────────────────────────────────────────────────────────────
def fetch_all_jobs():
    all_jobs, seen = [], set()

    # Switzerland via Adzuna
    print("\n── Switzerland (Adzuna) ─────────────────────────")
    for country, query in ADZUNA_SEARCHES:
        print(f"  Fetching [{country}] {query} …")
        for j in fetch_adzuna(country, query):
            if j['id'] not in seen:
                seen.add(j['id']); all_jobs.append(j)
        time.sleep(0.4)

    # Dubai: merge from curated sample (Emirates, Accenture, Noon, KPMG, etc.)
    sample_path = os.path.join(os.path.dirname(__file__), 'data', 'sample_jobs.json')
    with open(sample_path) as f:
        for j in json.load(f):
            if j['id'] not in seen:
                seen.add(j['id']); all_jobs.append(j)

    all_jobs.sort(key=lambda j: j.get('salary_usd_annual', 0), reverse=True)
    swiss = sum(1 for j in all_jobs if j.get('country') == 'Switzerland')
    dubai = sum(1 for j in all_jobs if j.get('country') == 'UAE')
    print(f"\n  ✓ Total: {len(all_jobs)} jobs  |  Switzerland (live): {swiss}  |  Dubai (curated): {dubai}")
    return all_jobs

if __name__ == '__main__':
    jobs = fetch_all_jobs()
    out  = os.path.join(os.path.dirname(__file__), 'data', 'live_jobs.json')
    with open(out, 'w') as f:
        json.dump(jobs, f, indent=2)
    print(f"  Saved → {out}")
