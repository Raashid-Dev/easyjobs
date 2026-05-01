import os, json, subprocess, re
from datetime import datetime, timezone
from flask import Flask, render_template, jsonify, request, redirect

app = Flask(__name__)
BASE        = os.path.dirname(__file__)
LIVE_FILE   = os.path.join(BASE, 'data', 'live_jobs.json')
SAMPLE_FILE = os.path.join(BASE, 'data', 'sample_jobs.json')
TS_FILE     = os.path.join(BASE, 'data', 'last_updated.json')

# ── Spam / relevance filters ─────────────────────────────────────────────────
TITLE_KW = [
    'analytic', 'analytics', 'data ', 'data-', ' data', 'intelligence', ' bi ',
    'dashboard', 'reporting', 'report', 'insight', 'kpi', 'digital marketing',
    'seo', 'sem', 'sql', 'power bi', 'tableau', 'looker', 'google analytics',
    'ga4', 'gtm', 'tag manager', 'marketing analytics', 'web analytics',
    'performance marketing', 'crm', 'visualization', 'bigquery', 'measurement',
    'tracking', 'attribution', 'conversion rate', 'cro',
    'mis ', 'information system', 'business analyst', 'growth analyst',
    'growth manager', 'media analyst', 'performance analyst',
    # Operations
    'operations manager', 'revenue operations', 'marketing operations',
    'business operations', 'growth operations', 'operations analyst',
    'ops manager', 'ops lead', 'operations lead',
    # Backend / Engineering
    'backend developer', 'backend engineer', 'full stack developer',
    'fullstack developer', 'data engineer', 'analytics engineer',
    'software developer', 'python developer', 'node.js developer',
    'api developer', 'platform engineer', 'engineering manager',
]
DESC_KW = [
    'google analytics', 'ga4', 'power bi', 'tableau', 'looker', 'bigquery',
    'sql', 'dashboard', 'kpi', 'analytics', 'data analysis', 'reporting',
    'digital marketing', 'seo', 'tracking', 'attribution', 'conversion',
    'insight', 'metric', 'measurement', 'operations', 'backend',
    'data engineer', 'revenue operations', 'python', 'api',
]
HARD_BLOCK_TITLE = [
    'driver', 'delivery boy', 'warehouse', 'nurse', 'teacher',
    'security guard', 'cook ', 'chef', 'cashier', 'receptionist',
    'electrician', 'plumber', 'mechanic', 'telemarketing', 'house maid',
    'domestic', 'factory worker', 'labourer', 'forklift',
    'content writer', 'copywriter', 'solicitor', 'lawyer', 'legal counsel',
    'construction manager', 'pre construction', 'quantity surveyor',
    'customer engineer', 'finance business partner', 'management accounting',
    'market risk controller', 'java developer', 'kotlin developer',
    'cloud engineer', 'sap ewm', 'sap bpa', 'generative ai engineer',
    'campus recruiter', 'associate (ca)', 'managing partner - banking',
]
SPAM_COMPANIES = ['testhiring', 'flat fee recruiter', 'wynwood tech']

def is_spam(j):
    title   = (j.get('title') or '').lower().strip()
    desc    = (j.get('description') or '').lower().strip()
    company = (j.get('company') or '').lower().strip()
    if not company or company == 'unknown':         return True
    if len(desc) < 40:                              return True
    if any(k in title for k in HARD_BLOCK_TITLE):  return True
    if any(k in company for k in SPAM_COMPANIES):  return True
    title_hit = any(k in title for k in TITLE_KW)
    desc_hits = sum(1 for k in DESC_KW if k in desc)
    if not title_hit and desc_hits < 2:             return True
    return False

def dedup_jobs(jobs):
    seen = {}
    for j in jobs:
        key = (
            (j.get('title') or '').strip().lower(),
            (j.get('company') or '').strip().lower()
        )
        existing = seen.get(key)
        if existing is None:
            seen[key] = j
        else:
            if (j.get('posted_days_ago') or 999) < (existing.get('posted_days_ago') or 999):
                seen[key] = j
    return list(seen.values())

def load_jobs():
    path = LIVE_FILE if os.path.exists(LIVE_FILE) else SAMPLE_FILE
    with open(path) as f:
        raw = json.load(f)
    filtered = [j for j in raw if not is_spam(j)]
    # Remove stale jobs older than 6 months (180 days)
    filtered = [j for j in filtered if (j.get('posted_days_ago') or 0) <= 180]
    # Remove fictional sample/curated jobs (id starts with job_) and
    # any job whose apply_url is a Google search (no real posting URL)
    filtered = [j for j in filtered
                if not (j.get('id','').startswith('job_'))
                and not (j.get('apply_url','').startswith('https://www.google.com/search'))]
    return dedup_jobs(filtered)

def save_timestamp(count, source='Adzuna'):
    ts = {'timestamp': datetime.now(timezone.utc).isoformat(), 'count': count, 'source': source}
    with open(TS_FILE, 'w') as f:
        json.dump(ts, f)

# ── Routes ───────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')

# Redirect old /jobs and /dashboard URLs to new root
@app.route('/jobs')
@app.route('/dashboard')
@app.route('/login')
@app.route('/logout')
def legacy_redirect():
    return redirect('/', 301)

@app.route('/api/jobs')
def get_jobs():
    jobs  = load_jobs()
    q     = request.args.get('q', '').lower()
    loc   = request.args.get('location', '')
    min_s = request.args.get('min_salary', 0, type=int)
    if q:
        jobs = [j for j in jobs if
                q in (j.get('title') or '').lower()
                or q in (j.get('company') or '').lower()
                or q in ' '.join(j.get('skills_required', [])).lower()
                or q in (j.get('description') or '').lower()]
    if loc and loc != 'all':
        jobs = [j for j in jobs if
                loc.lower() in (j.get('city') or '').lower()
                or loc.lower() in (j.get('country') or '').lower()]
    if min_s:
        jobs = [j for j in jobs if j.get('salary_usd_annual', 0) >= min_s]
    source = 'live' if os.path.exists(LIVE_FILE) else 'sample'
    return jsonify({'jobs': jobs, 'total': len(jobs), 'source': source})

@app.route('/api/refresh', methods=['POST'])
def refresh():
    try:
        result = subprocess.run(
            ['python3', os.path.join(BASE, 'fetch_jobs.py')],
            capture_output=True, text=True, timeout=180
        )
        if result.returncode == 0:
            jobs = load_jobs()
            save_timestamp(len(jobs), 'Adzuna Live')
            return jsonify({'status': 'success', 'count': len(jobs), 'log': result.stdout})
        return jsonify({'status': 'error', 'message': result.stderr}), 500
    except subprocess.TimeoutExpired:
        return jsonify({'status': 'error', 'message': 'Fetch timed out (>3 min)'}), 500
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/stats')
def stats():
    jobs = load_jobs()
    def city_match(j, name):
        return name in (j.get('city') or '').lower()
    return jsonify({
        'total':       len(jobs),
        'dubai':       sum(1 for j in jobs if city_match(j, 'dubai')),
        'switzerland': sum(1 for j in jobs if (j.get('country') or '').lower() == 'switzerland'),
        'germany':     sum(1 for j in jobs if (j.get('country') or '').lower() == 'germany'),
        'london':      sum(1 for j in jobs if city_match(j, 'london')),
        'saudi':       sum(1 for j in jobs if 'saudi' in (j.get('country') or '').lower()),
        'bahrain':     sum(1 for j in jobs if 'bahrain' in (j.get('country') or '').lower()),
        'qatar':       sum(1 for j in jobs if 'qatar' in (j.get('country') or '').lower()),
        'kuwait':      sum(1 for j in jobs if 'kuwait' in (j.get('country') or '').lower()),
        'middle_east': sum(1 for j in jobs if (j.get('country') or '').lower() in
                           ('uae', 'saudi arabia', 'bahrain', 'qatar', 'kuwait', 'oman')),
        'mumbai':      sum(1 for j in jobs if city_match(j, 'mumbai')),
        'delhi':       sum(1 for j in jobs if city_match(j, 'delhi') or city_match(j, 'gurgaon') or city_match(j, 'noida')),
        'bangalore':   sum(1 for j in jobs if city_match(j, 'bangalore') or city_match(j, 'bengaluru')),
        'singapore':   sum(1 for j in jobs if (j.get('country') or '').lower() == 'singapore'),
        'usa':         sum(1 for j in jobs if (j.get('country') or '').lower() == 'usa'),
        'canada':      sum(1 for j in jobs if (j.get('country') or '').lower() == 'canada'),
        'europe':      sum(1 for j in jobs if (j.get('country') or '').lower() in
                           ('united kingdom', 'uk', 'gb', 'germany', 'france', 'switzerland',
                            'netherlands', 'spain', 'italy', 'sweden', 'norway', 'denmark',
                            'belgium', 'austria', 'ireland', 'portugal')),
        'above_target':sum(1 for j in jobs if j.get('salary_usd_annual', 0) >= 180000),
        'avg_fit':     round(sum(j.get('fit_score', 85) for j in jobs) / len(jobs)) if jobs else 0,
        'source':      'live' if os.path.exists(LIVE_FILE) else 'sample'
    })

@app.route('/api/last_updated')
def last_updated():
    if os.path.exists(TS_FILE):
        with open(TS_FILE) as f:
            return jsonify(json.load(f))
    path = LIVE_FILE if os.path.exists(LIVE_FILE) else SAMPLE_FILE
    mtime = os.path.getmtime(path)
    dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    return jsonify({'timestamp': dt.isoformat(), 'count': 0, 'source': 'file mtime'})

@app.route('/health')
def health():
    return 'OK', 200

if __name__ == '__main__':
    if not os.path.exists(TS_FILE):
        jobs = load_jobs()
        save_timestamp(len(jobs), 'Sample data')
    port = int(os.environ.get('PORT', 5050))
    print(f"\n  EasyJobs running at  http://127.0.0.1:{port}\n")
    app.run(debug=False, host='0.0.0.0', port=port)
