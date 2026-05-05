import os, json, subprocess, re, functools
from datetime import datetime, timezone
from flask import (Flask, render_template, jsonify, request,
                   redirect, url_for, session, flash)
from models import (init_db, generate_otp, store_otp, verify_otp,
                    get_or_create_user, update_profile, get_user_by_id,
                    log_job_view, log_apply_click, analytics_summary,
                    INDIAN_CITIES, QUALIFICATIONS, GENDERS)

# ── Load .env file (no extra package needed) ──────────────────────────────────
_env_file = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(_env_file):
    with open(_env_file) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _k, _v = _line.split('=', 1)
                os.environ.setdefault(_k.strip(), _v.strip())

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'easyjobs-secret-2026-change-me')

BASE        = os.path.dirname(__file__)
LIVE_FILE   = os.path.join(BASE, 'data', 'live_jobs.json')
SAMPLE_FILE = os.path.join(BASE, 'data', 'sample_jobs.json')
TS_FILE     = os.path.join(BASE, 'data', 'last_updated.json')

# SMS — 2Factor.in (India OTP, ₹0.15/SMS, no DLT needed)
TWO_FACTOR_KEY = os.environ.get('TWO_FACTOR_KEY', '')
ADMIN_KEY      = os.environ.get('ADMIN_KEY', 'admin2026')

# ── Spam / relevance filters ─────────────────────────────────────────────────
TITLE_KW = [
    # Analytics / BI / Data
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
    'ops manager', 'ops lead', 'operations lead', 'operations director',
    'head of operations', 'chief operating',
    # Backend / Engineering
    'backend developer', 'backend engineer', 'full stack developer',
    'fullstack developer', 'data engineer', 'analytics engineer',
    'software developer', 'python developer', 'node.js developer',
    'api developer', 'platform engineer', 'engineering manager',
    # HR / Talent / People
    'hr manager', 'hr director', 'hr business partner', 'hrbp',
    'talent acquisition', 'talent management', 'recruitment manager', 'recruiter',
    'people manager', 'people operations', 'human resources', 'workforce',
    'learning & development', 'l&d manager', 'compensation', 'benefits manager',
    'hr analytics', 'people analytics', 'head of hr', 'chief people', 'head of talent',
    # CRM / Customer Success
    'crm manager', 'crm director', 'crm analyst', 'crm lead', 'salesforce', 'hubspot',
    'customer success manager', 'client success', 'cx manager', 'customer experience manager',
    # Sales / Business Development
    'sales manager', 'sales director', 'head of sales', 'vp sales',
    'business development manager', 'business development director', 'bdm',
    'account executive', 'enterprise sales', 'sales analytics', 'sales operations',
    'commercial manager', 'commercial director', 'revenue manager',
    'regional sales', 'national sales', 'country manager',
    # Retail / Category / E-commerce
    'store manager', 'retail manager', 'retail director', 'head of retail',
    'category manager', 'merchandising manager', 'ecommerce manager', 'e-commerce manager',
    'trade marketing', 'channel manager', 'omnichannel', 'buying manager',
    # Finance / FP&A
    'finance manager', 'finance director', 'head of finance', 'chief financial',
    'financial analyst', 'financial planning', 'fp&a', 'treasury manager',
    'financial controller', 'corporate finance', 'investment analyst',
    'management reporting', 'financial reporting',
    # Product / Marketing / Brand
    'product manager', 'product director', 'head of product', 'vp product',
    'marketing manager', 'marketing director', 'head of marketing', 'cmo',
    'brand manager', 'brand director', 'growth marketing', 'content marketing',
    'email marketing manager', 'performance marketing manager',
    # Fresher / Entry-level
    'junior analyst', 'junior data', 'junior hr', 'junior sales', 'junior finance',
    'junior marketing', 'junior product', 'graduate analyst', 'graduate trainee',
    'entry level analyst', 'entry level data', 'entry level hr', 'entry level sales',
    'fresher analyst', 'data analyst fresher', 'associate analyst', 'trainee analyst',
    # Blue Collar / Trades
    'driver', 'heavy driver', 'light driver', 'truck driver', 'bus driver',
    'electrician', 'electrical technician', 'electrical fitter',
    'plumber', 'plumbing technician',
    'ac technician', 'hvac technician', 'hvac engineer', 'refrigeration technician',
    'carpenter', 'woodworker',
    'welder', 'fabricator', 'fitter',
    'tailor', 'cutting master', 'pattern cutter', 'stitching',
    'painter ', 'spray painter',
    'mechanic', 'auto mechanic', 'vehicle technician',
    'maintenance technician', 'maintenance worker', 'handyman',
    'mason', 'tile layer',
]
DESC_KW = [
    'google analytics', 'ga4', 'power bi', 'tableau', 'looker', 'bigquery',
    'sql', 'dashboard', 'kpi', 'analytics', 'data analysis', 'reporting',
    'digital marketing', 'seo', 'tracking', 'attribution', 'conversion',
    'insight', 'metric', 'measurement', 'operations', 'backend',
    'data engineer', 'revenue operations', 'python', 'api',
    'recruitment', 'talent acquisition', 'human resources', 'salesforce', 'hubspot',
    'sales', 'business development', 'retail', 'store', 'ecommerce', 'crm',
    'finance', 'financial', 'treasury', 'fp&a', 'product manager', 'brand',
]
HARD_BLOCK_TITLE = [
    'delivery boy', 'warehouse', 'nurse', 'teacher',
    'security guard', 'cook ', 'chef', 'cashier', 'receptionist',
    'telemarketing', 'house maid',
    'domestic', 'factory worker', 'labourer', 'forklift',
    'content writer', 'copywriter', 'solicitor', 'lawyer', 'legal counsel',
    'construction manager', 'pre construction', 'quantity surveyor',
    'customer engineer', 'management accounting',
    'market risk controller', 'java developer', 'kotlin developer',
    'cloud engineer', 'sap ewm', 'sap bpa', 'generative ai engineer',
    'campus recruiter', 'associate (ca)', 'managing partner - banking',
]
BLUE_COLLAR_TITLE_KW = [
    'driver','heavy driver','light driver','truck driver','bus driver',
    'electrician','electrical technician','electrical fitter',
    'plumber','plumbing technician',
    'ac technician','hvac technician','hvac engineer','refrigeration technician',
    'carpenter','woodworker',
    'welder','fabricator','fitter',
    'tailor','cutting master','pattern cutter','stitching',
    'painter ','spray painter',
    'mechanic','auto mechanic','vehicle technician',
    'maintenance technician','maintenance worker','handyman',
    'mason','tile layer',
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

def _has_phone(j):
    return bool(j.get('has_phone')) or bool((j.get('hr_contact') or {}).get('phone'))

def load_jobs():
    path = LIVE_FILE if os.path.exists(LIVE_FILE) else SAMPLE_FILE
    with open(path) as f:
        raw = json.load(f)
    filtered = [j for j in raw if not is_spam(j)]
    filtered = [j for j in filtered if (j.get('posted_days_ago') or 0) <= 180]
    filtered = [j for j in filtered
                if not (j.get('id','').startswith('job_'))
                and not (j.get('apply_url','').startswith('https://www.google.com/search'))]
    return dedup_jobs(filtered)

def save_timestamp(count, source='Adzuna'):
    ts = {'timestamp': datetime.now(timezone.utc).isoformat(), 'count': count, 'source': source}
    with open(TS_FILE, 'w') as f:
        json.dump(ts, f)

# ── Auth helpers ──────────────────────────────────────────────────────────────

def send_otp_sms(mobile: str, otp: str) -> dict:
    """Send OTP via 2Factor.in — ₹0.15/SMS, no DLT needed.
    Always prints OTP to console as backup.
    Returns {'ok': True/False, 'msg': '...'}."""
    # Always log to console for debugging
    print(f"\n{'='*50}", flush=True)
    print(f"  OTP for {mobile}: {otp}", flush=True)
    print(f"{'='*50}\n", flush=True)

    if not TWO_FACTOR_KEY:
        print("[2Factor] No API key — using console mode", flush=True)
        return {'ok': True, 'msg': 'dev_console'}

    try:
        import urllib.request, ssl, certifi as _certifi

        # 2Factor expects 10-digit number (strip country code)
        num = mobile.lstrip('+')
        if num.startswith('91') and len(num) == 12:
            num = num[2:]

        url = f'https://2factor.in/API/V1/{TWO_FACTOR_KEY}/VOICE/{num}/{otp}'
        req = urllib.request.Request(url, method='GET')
        ssl_ctx = ssl.create_default_context(cafile=_certifi.where())
        resp = urllib.request.urlopen(req, timeout=10, context=ssl_ctx)
        d = json.loads(resp.read())
        print(f"[2Factor] status={d.get('Status')} details={d.get('Details')}", flush=True)

        if d.get('Status') == 'Success':
            return {'ok': True, 'msg': d.get('Details', 'sent')}
        return {'ok': False, 'msg': d.get('Details', 'Unknown error from 2Factor')}

    except urllib.error.HTTPError as e:
        body = e.read().decode(errors='replace')
        print(f"[2Factor] HTTP {e.code}: {body}", flush=True)
        return {'ok': False, 'msg': f'2Factor error: {body}'}
    except Exception as e:
        print(f"[2Factor] Exception: {e}", flush=True)
        return {'ok': False, 'msg': str(e)}

def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user_id'):
            return redirect(url_for('login_page'))
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        key = request.args.get('key') or request.form.get('key') or session.get('admin_key')
        if key != ADMIN_KEY:
            return render_template('admin_login.html'), 401
        session['admin_key'] = key
        return f(*args, **kwargs)
    return decorated

# ── Public pages ──────────────────────────────────────────────────────────────

@app.route('/')
def index():
    user = None
    if session.get('user_id'):
        user = get_user_by_id(session['user_id'])
    return render_template('index.html', user=user)

@app.route('/login')
def login_page():
    if session.get('user_id'):
        return redirect(url_for('index'))
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login_page'))

# ── OTP flow ──────────────────────────────────────────────────────────────────

@app.route('/auth/send-otp', methods=['POST'])
def send_otp():
    mobile = (request.json or request.form).get('mobile', '').strip()
    mobile = re.sub(r'\D', '', mobile)
    if not mobile or len(mobile) < 10:
        return jsonify({'ok': False, 'msg': 'Enter a valid 10-digit mobile number'}), 400
    if len(mobile) == 10:
        mobile = '91' + mobile   # prepend India country code
    otp = generate_otp(6)
    store_otp(mobile, otp)
    result = send_otp_sms(mobile, otp)
    if result['ok']:
        return jsonify({'ok': True, 'msg': 'OTP sent successfully'})
    return jsonify({'ok': False, 'msg': f'SMS delivery failed: {result["msg"]}'}), 500

@app.route('/auth/verify-otp', methods=['POST'])
def verify_otp_route():
    data   = request.json or request.form
    mobile = re.sub(r'\D', '', (data.get('mobile') or '').strip())
    otp    = (data.get('otp') or '').strip()
    if len(mobile) == 10:
        mobile = '91' + mobile
    if not mobile or not otp:
        return jsonify({'ok': False, 'msg': 'Mobile and OTP required'}), 400
    if not verify_otp(mobile, otp):
        return jsonify({'ok': False, 'msg': 'Invalid or expired OTP'}), 401
    user = get_or_create_user(mobile)
    session['user_id']    = user['id']
    session['user_mobile'] = mobile
    session.permanent     = True
    return jsonify({
        'ok':      True,
        'new_user': not user.get('is_complete'),
        'redirect': url_for('profile_page') if not user.get('is_complete') else url_for('index')
    })

# ── Profile ───────────────────────────────────────────────────────────────────

@app.route('/profile')
@login_required
def profile_page():
    user = get_user_by_id(session['user_id'])
    return render_template('profile.html', user=user,
                           cities=INDIAN_CITIES,
                           qualifications=QUALIFICATIONS,
                           genders=GENDERS)

@app.route('/profile', methods=['POST'])
@login_required
def save_profile():
    data  = request.json or request.form
    name  = (data.get('name') or '').strip()
    gender = data.get('gender', '')
    age   = data.get('age', 0)
    qual  = data.get('qualification', '')
    city  = data.get('city', '')
    errors = []
    if not name:           errors.append('Name is required')
    if gender not in GENDERS: errors.append('Select a valid gender')
    try:
        age = int(age)
        if not (14 <= age <= 80): errors.append('Age must be between 14 and 80')
    except (ValueError, TypeError):
        errors.append('Enter a valid age')
    if qual not in QUALIFICATIONS: errors.append('Select a valid qualification')
    if city not in INDIAN_CITIES:  errors.append('Select a valid city')
    if errors:
        return jsonify({'ok': False, 'errors': errors}), 400
    complete = update_profile(session['user_id'], name, gender, age, qual, city)
    return jsonify({'ok': True, 'complete': complete, 'redirect': url_for('index')})

# ── Activity tracking APIs ────────────────────────────────────────────────────

@app.route('/api/track/view', methods=['POST'])
def track_view():
    if not session.get('user_id'):
        return jsonify({'ok': False}), 401
    d = request.json or {}
    log_job_view(session['user_id'],
                 d.get('job_id',''), d.get('title',''),
                 d.get('company',''), d.get('country',''), d.get('category',''))
    return jsonify({'ok': True})

@app.route('/api/track/apply', methods=['POST'])
def track_apply():
    if not session.get('user_id'):
        return jsonify({'ok': False}), 401
    d = request.json or {}
    log_apply_click(session['user_id'],
                    d.get('job_id',''), d.get('title',''),
                    d.get('company',''), d.get('country',''), d.get('category',''))
    return jsonify({'ok': True})

# ── Admin analytics dashboard ─────────────────────────────────────────────────

@app.route('/admin')
@admin_required
def admin_dashboard():
    data = analytics_summary()
    return render_template('admin.html', data=data, admin_key=ADMIN_KEY)

@app.route('/api/admin/analytics')
@admin_required
def admin_analytics_api():
    return jsonify(analytics_summary())

@app.route('/api/admin/users')
@admin_required
def admin_users_api():
    from models import get_conn
    limit = request.args.get('limit', 20, type=int)
    conn  = get_conn()
    rows  = conn.execute(
        "SELECT * FROM users ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return jsonify({'users': [dict(r) for r in rows]})

# ── Existing job routes ───────────────────────────────────────────────────────

@app.route('/jobs')
@app.route('/dashboard')
def legacy_redirect():
    return redirect('/', 301)

# ── AI Resume Builder ─────────────────────────────────────────────────────────

@app.route('/resume-builder')
def resume_builder():
    user = None
    if session.get('user_id'):
        user = get_user_by_id(session['user_id'])
    return render_template('resume_builder.html', user=user)

# ── AI-powered resume parser (Gemini Flash — FREE up to 1500/day) ─────────────
_AI_RESUME_PROMPT = """You are a professional resume parser. Extract all information from the resume text and return ONLY a valid JSON object — no markdown fences, no explanation, just raw JSON.

Return exactly this structure:
{
  "name": "candidate full name only",
  "title": "current or most recent job title / professional headline",
  "email": "email address",
  "phone": "phone number with country code",
  "location": "city and country",
  "linkedin": "linkedin.com/in/... URL or empty string",
  "summary": "write a strong 2-4 sentence professional summary based on the resume",
  "experience": [
    {
      "role": "exact job title",
      "company": "company or organisation name",
      "period": "date range e.g. Jan 2022 – Present",
      "location": "city or Remote",
      "bullets": [
        "Strong achievement sentence starting with an action verb (Analyzed, Built, Led, etc.)"
      ]
    }
  ],
  "education": [
    {
      "degree": "full degree or qualification",
      "school": "institution name",
      "year": "year or range",
      "grade": "CGPA / percentage / distinction if present, else empty string"
    }
  ],
  "skills": ["SQL", "Power BI", "Python"],
  "languages": [
    {"lang": "English", "level": "Native"}
  ]
}

Critical rules:
- name: ONLY the person's name — never include email, phone, or address
- experience.bullets: complete professional sentences, never fragments; max 5 per role
- skills: only tool/technology/method names (1-4 words max); never full sentences or duties
- Improve any weak or fragmented bullet points into clear, professional achievement statements
- If a field is missing, use "" or []
- level must be one of: Native, Fluent, Professional, Conversational, Basic
"""

def _normalise_ai_result(parsed):
    """Ensure all required keys exist and types are correct."""
    parsed.setdefault('name', '')
    parsed.setdefault('title', '')
    parsed.setdefault('email', '')
    parsed.setdefault('phone', '')
    parsed.setdefault('location', '')
    parsed.setdefault('linkedin', '')
    parsed.setdefault('summary', '')
    parsed.setdefault('experience', [])
    parsed.setdefault('education', [])
    parsed.setdefault('skills', [])
    parsed.setdefault('languages', [])
    for exp in parsed.get('experience', []):
        exp.setdefault('role', '')
        exp.setdefault('company', '')
        exp.setdefault('period', '')
        exp.setdefault('location', '')
        if isinstance(exp.get('bullets'), str):
            exp['bullets'] = [b.strip() for b in exp['bullets'].split('\n') if b.strip()]
        exp.setdefault('bullets', [])
    for edu in parsed.get('education', []):
        edu.setdefault('degree', '')
        edu.setdefault('school', '')
        edu.setdefault('year', '')
        edu.setdefault('grade', '')
    return parsed


def _parse_resume_with_ai(raw_text: str):
    """Parse resume using Gemini Flash (free) or fall back to Claude Haiku."""
    gemini_key    = os.environ.get('GEMINI_API_KEY', '').strip()
    anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '').strip()

    # ── Try Gemini 2.5 Flash first (free tier: 1,500 req/day) ─────────────
    if gemini_key:
        try:
            import urllib.request, ssl, certifi as _certifi
            full_prompt = _AI_RESUME_PROMPT + '\n\nRESUME TEXT:\n' + raw_text[:8000]
            payload = json.dumps({
                'contents': [{'parts': [{'text': full_prompt}]}],
                'generationConfig': {
                    'temperature': 0.1,
                    'maxOutputTokens': 8192,   # resume JSON can be 1500+ tokens
                }
            }).encode()
            url = (f'https://generativelanguage.googleapis.com/v1beta/models/'
                   f'gemini-2.5-flash:generateContent?key={gemini_key}')
            req = urllib.request.Request(url, data=payload,
                                         headers={'content-type': 'application/json'})
            ssl_ctx = ssl.create_default_context(cafile=_certifi.where())
            resp    = urllib.request.urlopen(req, timeout=45, context=ssl_ctx)
            result  = json.loads(resp.read())
            raw_out = result['candidates'][0]['content']['parts'][0]['text']
            # Robust JSON extraction: find the outermost { ... } block
            m = re.search(r'\{[\s\S]*\}', raw_out)
            if not m:
                raise ValueError(f'No JSON object found in response: {raw_out[:200]}')
            parsed  = json.loads(m.group())
            parsed  = _normalise_ai_result(parsed)
            print(f"[AI Parser] ✓ Gemini 2.5 Flash — {len(parsed['experience'])} jobs, "
                  f"{len(parsed['skills'])} skills", flush=True)
            return parsed
        except Exception as e:
            print(f"[AI Parser] Gemini failed ({e}), trying Anthropic…", flush=True)

    # ── Fallback: Claude Haiku (~₹0.13/parse) ──────────────────────────────
    if anthropic_key:
        try:
            import urllib.request, ssl, certifi as _certifi
            payload = json.dumps({
                'model': 'claude-3-haiku-20240307',
                'max_tokens': 2048,
                'messages': [{'role': 'user',
                               'content': _AI_RESUME_PROMPT + '\n\nRESUME TEXT:\n' + raw_text[:7000]}]
            }).encode()
            req = urllib.request.Request(
                'https://api.anthropic.com/v1/messages', data=payload,
                headers={'x-api-key': anthropic_key,
                         'anthropic-version': '2023-06-01',
                         'content-type': 'application/json'})
            ssl_ctx = ssl.create_default_context(cafile=_certifi.where())
            resp    = urllib.request.urlopen(req, timeout=30, context=ssl_ctx)
            result  = json.loads(resp.read())
            raw_out = result['content'][0]['text']
            m       = re.search(r'\{[\s\S]*\}', raw_out)
            if not m:
                raise ValueError('No JSON in Claude response')
            parsed  = _normalise_ai_result(json.loads(m.group()))
            print(f"[AI Parser] ✓ Claude Haiku — {len(parsed['experience'])} jobs, "
                  f"{len(parsed['skills'])} skills", flush=True)
            return parsed
        except Exception as e:
            print(f"[AI Parser] Claude failed ({e})", flush=True)

    return None  # Both failed / no keys set — caller will use regex fallback


def _fix_pdf_text(text):
    """Post-process raw PDF text to fix common extraction artifacts."""
    import re
    # 1. Fix ligatures and unicode punctuation
    for bad, good in [
        ('ﬁ','fi'),('ﬂ','fl'),('ﬀ','ff'),('ﬃ','ffi'),('ﬄ','ffl'),
        ('’',"'"),('‘',"'"),('“','"'),('”','"'),
        ('–','-'),('—','-'),(' ',' '),('​',''),
        (''',"'"),(''',"'"),('–','-'),('—','-'),(' ',' '),
    ]:
        text = text.replace(bad, good)
    # 2. Fix character-spaced words: "M a n a g e r" / "Managem e n t"
    #    After a letter, 3+ space-separated single chars → merge with preceding text
    text = re.sub(
        r'(?<=[A-Za-z])( [A-Za-z]){3,}',
        lambda m: m.group().replace(' ', ''),
        text
    )
    # 3. Fix standalone letter sequences like "C R M", "S E O", "U A E"
    text = re.sub(
        r'\b([A-Z] ){2,}[A-Z]\b',
        lambda m: m.group().replace(' ', ''),
        text
    )
    # 4. Collapse runs of spaces to single space (but keep newlines)
    text = re.sub(r'[ \t]{2,}', ' ', text)
    return text


def _extract_page_text(page):
    """Extract text from a pdfplumber page handling both single- and multi-column layouts."""
    # x_tolerance=6 merges characters that are slightly spaced apart (kerning artifacts)
    words = page.extract_words(x_tolerance=6, y_tolerance=3, keep_blank_chars=False)
    if not words:
        return page.extract_text(x_tolerance=6, y_tolerance=3) or ''

    page_w = float(page.width)
    mid    = page_w / 2

    # Detect two-column layout: significant words on both sides with a gap in the middle
    left_w  = [w for w in words if float(w['x1']) < mid * 0.85]
    right_w = [w for w in words if float(w['x0']) > mid * 1.15]
    is_two_col = len(left_w) >= 8 and len(right_w) >= 8

    def words_to_lines(word_list):
        line_map = {}
        for w in word_list:
            y = round(float(w['top']) / 4) * 4
            line_map.setdefault(y, []).append(w)
        lines = []
        for y in sorted(line_map):
            row = sorted(line_map[y], key=lambda w: float(w['x0']))
            lines.append(' '.join(w['text'] for w in row))
        return lines

    if is_two_col:
        # Process left column first, then right — keeps sections in logical order
        col_split = mid
        left_side  = [w for w in words if float(w['x0']) < col_split]
        right_side = [w for w in words if float(w['x0']) >= col_split]
        return '\n'.join(words_to_lines(left_side) + words_to_lines(right_side))
    else:
        return '\n'.join(words_to_lines(words))


@app.route('/api/resume/parse', methods=['POST'])
def parse_resume_api():
    """Accept uploaded resume PDF/DOCX, extract and structure text."""
    import io
    f = request.files.get('resume')
    if not f:
        return jsonify({'ok': False, 'msg': 'No file uploaded'}), 400
    fname = f.filename.lower()
    raw_bytes = f.read()
    raw_text = ''
    try:
        if fname.endswith('.pdf'):
            import pdfplumber
            with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
                pages = [_extract_page_text(p) for p in pdf.pages]
            raw_text = _fix_pdf_text('\n'.join(p for p in pages if p))
        elif fname.endswith('.docx'):
            try:
                import docx as _docx
                doc = _docx.Document(io.BytesIO(raw_bytes))
                raw_text = '\n'.join(p.text for p in doc.paragraphs)
            except ImportError:
                return jsonify({'ok': False, 'msg': 'DOCX support not installed. Please upload PDF.'}), 400
        else:
            raw_text = raw_bytes.decode('utf-8', errors='replace')
    except Exception as e:
        return jsonify({'ok': False, 'msg': f'Could not read file: {e}'}), 400

    if not raw_text.strip():
        return jsonify({'ok': False, 'msg': 'Could not extract text. Try a different PDF or use manual entry.'}), 400

    # ── Try AI parser first (Claude Haiku) ──────────────────────────────────
    structured = _parse_resume_with_ai(raw_text)
    if structured is None:
        # Fallback: regex-based parser (no API key required)
        print('[Resume] Using regex fallback parser', flush=True)
        structured = _parse_resume_text(raw_text)
    return jsonify({'ok': True, 'data': structured})


@app.route('/api/resume/enhance-photo', methods=['POST'])
def enhance_photo_api():
    """Use Stability AI SDXL v1 img2img to add professional suit & studio background."""
    import io, base64, json as _json
    stab_key = os.environ.get('STABILITY_API_KEY', '').strip()
    if not stab_key:
        return jsonify({'ok': False, 'no_key': True,
                        'msg': 'Add STABILITY_API_KEY to .env — free credits at platform.stability.ai'})

    f = request.files.get('photo')
    if not f:
        return jsonify({'ok': False, 'msg': 'No photo uploaded'}), 400

    img_bytes = f.read()

    # Resize to 1024×1024 for SDXL (best results)
    try:
        from PIL import Image as _PIL
        img = _PIL.open(io.BytesIO(img_bytes)).convert('RGB')
        img = img.resize((1024, 1024), _PIL.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format='JPEG', quality=92)
        img_bytes = buf.getvalue()
        print('[Photo] Resized to 1024×1024 for SDXL', flush=True)
    except Exception as pil_err:
        print(f'[Photo] PIL not available, using original: {pil_err}', flush=True)

    try:
        import requests as _req
        import warnings
        warnings.filterwarnings('ignore')   # suppress LibreSSL warnings

        print(f'[Photo] Sending {len(img_bytes)/1024:.0f}KB to Stability AI SDXL…', flush=True)

        resp = _req.post(
            'https://api.stability.ai/v1/generation/stable-diffusion-xl-1024-v1-0/image-to-image',
            headers={
                'Authorization': f'Bearer {stab_key}',
                'Accept':        'application/json',
                'User-Agent':    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            },
            files={
                'init_image': ('photo.jpg', img_bytes, 'image/jpeg'),
            },
            data={
                'init_image_mode':        'IMAGE_STRENGTH',
                'image_strength':         '0.35',
                'text_prompts[0][text]':  (
                    'professional corporate headshot portrait photograph, same person same face, '
                    'wearing a sharp charcoal grey formal business suit, crisp white dress shirt, '
                    'dark silk tie, plain neutral studio background with soft gradient, '
                    'professional studio lighting with soft fill light, photorealistic, '
                    '8K ultra detailed, sharp focus, LinkedIn profile photo, formal portrait'
                ),
                'text_prompts[0][weight]': '1',
                'text_prompts[1][text]':  (
                    'casual clothing, t-shirt, hoodie, jeans, shorts, outdoor background, '
                    'trees, street, buildings, blur, cartoon, drawing, anime, painting, '
                    'distorted face, extra limbs, low quality, watermark, text, ugly, '
                    'bad anatomy, deformed, multiple people'
                ),
                'text_prompts[1][weight]': '-1',
                'cfg_scale':  '9',
                'samples':    '1',
                'steps':      '40',
            },
            timeout=120,
            verify=False,   # avoid LibreSSL cert issue on macOS
        )

        if resp.status_code != 200:
            msg = resp.text[:400]
            print(f'[Photo] ✗ HTTP {resp.status_code}: {msg}', flush=True)
            return jsonify({'ok': False, 'msg': f'API {resp.status_code}: {msg}'})

        result   = resp.json()
        artifact = result.get('artifacts', [{}])[0]
        if artifact.get('finishReason') == 'ERROR':
            msg = artifact.get('errorMessage', 'Unknown AI error')
            return jsonify({'ok': False, 'msg': msg})
        b64 = artifact.get('base64', '')
        if not b64:
            return jsonify({'ok': False, 'msg': 'No image returned by API'})

        print(f'[Photo] ✓ Enhanced — {len(b64)//1024}KB', flush=True)
        return jsonify({'ok': True, 'image': f'data:image/png;base64,{b64}'})

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'ok': False, 'msg': str(e)})


# ── Section header registry ────────────────────────────────────────────────────
_SECTIONS = {
    'summary':   ['summary','professional summary','career objective','objective',
                  'professional profile','about me','profile','personal statement',
                  'career summary','executive summary','professional overview',
                  'career profile','about','intro','introduction'],
    'experience':['experience','work experience','professional experience',
                  'employment history','work history','career history','employment',
                  'professional background','internship','internships',
                  'work profile','professional highlights','professional history',
                  'professional details','job experience','career details',
                  'relevant experience','industry experience','experience details',
                  'work details','employment details','job history'],
    'education': ['education','educational qualifications','academic background',
                  'educational background','academics','qualifications',
                  'academic qualifications','academic details','educational details',
                  'educational profile','academic profile','academic history',
                  'education details','educational history'],
    'skills':    ['skills','technical skills','core competencies','key skills',
                  'competencies','skills & expertise','technical expertise',
                  'tools & technologies','technologies','areas of expertise',
                  'skill set','technical competencies','skills summary',
                  'professional skills','key competencies','core skills',
                  'technical proficiency','skills and expertise',
                  'skills & competencies'],
    'certifications':['certifications','certificates','professional certifications',
                      'certifications & training','licenses & certifications',
                      'certification','training','professional development',
                      'courses','additional qualifications'],
    'languages': ['languages','language proficiency','languages known',
                  'language skills','languages & proficiency'],
    'achievements':['achievements','accomplishments','awards','honors',
                    'awards & achievements','key achievements','highlights',
                    'notable achievements'],
    'projects':  ['projects','key projects','notable projects','project experience',
                  'project details','major projects'],
}

def _find_sections(lines):
    """Return sorted list of (line_idx, section_name) for each header found.
    Handles decorative chars, all-caps, multiple spaces, and common variations."""
    import re
    found = []
    seen_sections = set()
    for i, line in enumerate(lines):
        # Aggressive normalisation: strip ALL non-alphanumeric chars,
        # lowercase, collapse spaces — handles "━━ WORK EXPERIENCE ━━" etc.
        clean = re.sub(r'[^a-z0-9\s]', ' ', line.lower())
        clean = re.sub(r'\s+', ' ', clean).strip()
        if not clean or len(clean) > 55:
            continue
        for sec, keywords in _SECTIONS.items():
            if sec in seen_sections:
                continue
            if clean in keywords:
                found.append((i, sec))
                seen_sections.add(sec)
                break
    return sorted(found, key=lambda x: x[0])

def _section_content(lines, section_positions, name):
    for idx, (start, sname) in enumerate(section_positions):
        if sname == name:
            end = section_positions[idx+1][0] if idx+1 < len(section_positions) else len(lines)
            return '\n'.join(lines[start+1:end]).strip()
    return ''

def _parse_experience(text):
    import re
    if not text.strip(): return []
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    entries, cur = [], {'role':'','company':'','period':'','location':'','bullets':[]}
    BULLET = set('•–−-·◆▪►■○→*')
    DATE_RE = re.compile(
        r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|'
        r'january|february|march|april|june|july|august|september|'
        r'october|november|december|\d{4}|present|current|till\s*date)\b', re.I)

    def save():
        if cur.get('role') or cur.get('company') or cur.get('bullets'):
            c = dict(cur); c['bullets'] = c['bullets'][:5]; entries.append(c)

    for line in lines:
        is_bullet = line[0] in BULLET if line else False
        has_date  = bool(DATE_RE.search(line))

        if is_bullet:
            b = re.sub(r'^[•–−\-·◆▪►■○→*\s]+', '', line).strip()
            if b and len(b) > 3: cur['bullets'].append(b)

        elif has_date and len(line) < 90:
            if not cur['period']:
                cur['period'] = line
            else:
                save()
                cur = {'role':'','company':'','period':line,'location':'','bullets':[]}

        elif not cur['role'] and len(line) < 70:
            cur['role'] = line
        elif not cur['company'] and len(line) < 70 and cur['role']:
            cur['company'] = line
        elif len(line) > 25:
            cur['bullets'].append(line)

    save()
    return entries[:6]

def _parse_education(text):
    import re
    if not text.strip(): return []
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    entries, cur = [], {'degree':'','school':'','year':'','grade':''}
    YEAR_RE  = re.compile(r'\b(19|20)\d{2}', re.I)
    DEG_KW   = ['b.tech','btech','b.e','be ','m.tech','mba','bsc','b.sc','ba ',
                'm.a','m.sc','phd','msc','bachelor','master','diploma','b.e.',
                'ssc','hsc','10th','12th','b.com','bcom','bca','mca','b.arch',
                'class x','class xii','class 10','class 12','secondary',
                'higher secondary','high school','intermediate','matriculation',
                'graduation','post graduation','pg diploma']
    GRADE_KW = ['cgpa','gpa','%','percent','grade','distinction','first class',
                'second class','pass','aggregate']

    for line in lines:
        low = line.lower()
        is_deg   = any(k in low for k in DEG_KW)
        has_year = bool(YEAR_RE.search(line))
        is_grade = any(k in low for k in GRADE_KW)

        if is_deg:
            if cur.get('degree'):
                entries.append(cur)
                cur = {'degree':'','school':'','year':'','grade':''}
            cur['degree'] = line
        elif has_year and not cur.get('year'):
            cur['year'] = line
        elif is_grade and not cur.get('grade'):
            cur['grade'] = line
        elif not cur.get('school') and len(line) < 80 and not has_year:
            cur['school'] = line

    if cur.get('degree') or cur.get('school'):
        entries.append(cur)
    return entries[:5]

def _parse_skills(text):
    import re
    if not text.strip(): return []
    # Normalise separators — bullets, pipes, newlines, tabs all become commas
    text = re.sub(r'[•·◆▪►■○→|\\]', ',', text)
    text = text.replace('\n', ',').replace('\t', ',')
    # Words/phrases that indicate a sentence fragment, not a skill
    STOP = re.compile(
        r'\b(and\b|the\b|for\b|with\b|\bto\b|\bof\b|\bin\b|\bat\b|\bby\b|'
        r'from\b|that\b|this\b|have\b|has\b|are\b|was\b|were\b|will\b|'
        r'been\b|used\b|using\b|able\b|also\b|which\b|where\b|when\b|'
        r'how\b|why\b|what\b|including\b|such\b|\bas\b|\bi\b|\bmy\b|'
        r'\bwe\b|their\b|them\b|into\b|across\b|helped\b|worked\b|'
        r'provided\b|supported\b|managed\b|developed\b|created\b|'
        r'identified\b|collected\b|organized\b|generated\b|reducing\b|'
        r'improving\b|ensuring\b|enabling\b|delivering\b|achieved\b)')
    # Also reject anything ending with punctuation (sentence-like)
    SENTENCE_END = re.compile(r'[.,;!?]$')
    seen, result = set(), []
    for tok in text.split(','):
        t = tok.strip().strip('–-•*()[]{}"\'').strip()
        if len(t) < 2 or len(t) > 45: continue
        words = t.split()
        if len(words) > 5: continue                  # too long = phrase/sentence
        if STOP.search(t.lower()): continue          # contains stop word
        if SENTENCE_END.search(t): continue          # ends like a sentence
        if re.match(r'^\d+$', t): continue           # bare number
        if re.match(r'^\W+$', t): continue           # only symbols
        k = t.lower()
        if k not in seen:
            seen.add(k)
            result.append(t)
    return result[:30]

def _parse_languages(text):
    import re
    if not text.strip(): return []
    lines = re.split(r'[,\n]', text.replace('|','\n'))
    LEVELS = ['native','mother tongue','fluent','professional','conversational',
              'basic','beginner','advanced','intermediate','proficient']
    result = []
    for raw in lines:
        raw = raw.strip()
        if not raw or len(raw) < 2: continue
        low  = raw.lower()
        lvl  = 'Fluent'
        name = raw
        for l in LEVELS:
            if l in low:
                lvl  = l.replace('mother tongue','Native').capitalize()
                name = re.sub(re.escape(l),'', raw, flags=re.I).strip(' –-:,()[]')
                break
        name = name.strip()
        if name and len(name) < 35:
            result.append({'lang': name, 'level': lvl})
    return result[:6]

def _parse_resume_text(text):
    import re
    # Fix common PDF ligature/encoding issues
    for bad, good in [('ﬁ','fi'),('ﬂ','fl'),('ﬀ','ff'),('ﬃ','ffi'),('ﬄ','ffl'),
                      ('’',"'"),('‘',"'"),('–','-'),('—','-'),
                      (' ',' ')]:
        text = text.replace(bad, good)

    lines = [l.strip() for l in text.splitlines() if l.strip()]
    section_positions = _find_sections(lines)

    first = section_positions[0][0] if section_positions else len(lines)
    header = lines[:first]
    htext  = '\n'.join(header)

    # Name — first line that has no digits/email/url
    name = ''
    for l in header:
        if not re.search(r'@|http|linkedin|github|\+?\d[\d ]{7,}', l.lower()):
            if len(l) > 2 and len(l) < 60:
                name = l; break

    email    = re.search(r'[\w.\-+]+@[\w.\-]+\.[a-zA-Z]{2,}', htext)
    email    = email.group() if email else ''
    phone_m  = re.search(r'(\+?[\d][\d\s\-\(\)\.]{7,16}[\d])', htext)
    phone    = phone_m.group().strip() if phone_m else ''
    linkedin = re.search(r'linkedin\.com/in/[\w\-]+', htext, re.I)
    linkedin = linkedin.group() if linkedin else ''

    title = ''
    role_kw = ['analyst','manager','engineer','developer','designer','consultant',
               'director','specialist','lead','executive','architect','head',
               'coordinator','associate','officer','intern','professional',
               'accountant','marketer','recruiter']
    for l in header[1:6]:
        if any(k in l.lower() for k in role_kw) and 5 < len(l) < 85:
            title = l; break

    summary  = _section_content(lines, section_positions, 'summary')
    exp_raw  = _section_content(lines, section_positions, 'experience')
    edu_raw  = _section_content(lines, section_positions, 'education')
    skl_raw  = _section_content(lines, section_positions, 'skills')
    lang_raw = _section_content(lines, section_positions, 'languages')
    cert_raw = _section_content(lines, section_positions, 'certifications')

    return {
        'name':           name,
        'title':          title,
        'email':          email,
        'phone':          phone,
        'linkedin':       linkedin,
        'location':       '',
        'summary':        summary,
        'experience':     _parse_experience(exp_raw),
        'education':      _parse_education(edu_raw),
        'skills':         _parse_skills(skl_raw),
        'languages':      _parse_languages(lang_raw),
        'certifications': cert_raw,
    }

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

FETCH_LOG = os.path.join(BASE, 'data', 'fetch_log.txt')

def _run_fetch():
    import shutil
    py = shutil.which('python3.11') or shutil.which('python3') or 'python3'
    with open(FETCH_LOG, 'w') as log:
        result = subprocess.run(
            [py, os.path.join(BASE, 'fetch_jobs.py')],
            stdout=log, stderr=subprocess.STDOUT, timeout=1800
        )
    if result.returncode == 0:
        jobs = load_jobs()
        save_timestamp(len(jobs), 'Live (multi-source)')

_fetch_thread = None

@app.route('/api/refresh', methods=['POST'])
def refresh():
    global _fetch_thread
    if _fetch_thread and _fetch_thread.is_alive():
        return jsonify({'status': 'running',
                        'message': 'Fetch already in progress — check back in a few minutes'})
    import threading
    _fetch_thread = threading.Thread(target=_run_fetch, daemon=True)
    _fetch_thread.start()
    return jsonify({'status': 'started',
                    'message': 'Fetching jobs in background (10-15 min). Page will auto-refresh.'})

@app.route('/api/refresh/status')
def refresh_status():
    global _fetch_thread
    running = bool(_fetch_thread and _fetch_thread.is_alive())
    jobs = load_jobs() if not running else []
    return jsonify({
        'running':  running,
        'count':    len(jobs) if not running else None,
        'log_tail': open(FETCH_LOG).readlines()[-5:] if os.path.exists(FETCH_LOG) else []
    })

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
        'above_target': sum(1 for j in jobs if j.get('salary_usd_annual', 0) >= 180000),
        'avg_fit':      round(sum(j.get('fit_score', 85) for j in jobs) / len(jobs)) if jobs else 0,
        'source':       'live' if os.path.exists(LIVE_FILE) else 'sample'
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
    init_db()
    if not os.path.exists(TS_FILE):
        jobs = load_jobs()
        save_timestamp(len(jobs), 'Sample data')
    port = int(os.environ.get('PORT', 5050))
    print(f"\n  EasyJobs running at  http://127.0.0.1:{port}\n")
    app.run(debug=False, host='0.0.0.0', port=port)
