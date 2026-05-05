"""
models.py — EasyJobs SQLite DB schema + helpers
"""
import sqlite3, os, random, string
from datetime import datetime, timezone, timedelta

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, 'data', 'users.db')

INDIAN_CITIES = [
    # Metros
    "Mumbai","Delhi","Bangalore","Hyderabad","Chennai","Kolkata","Pune","Ahmedabad",
    # Tier 2
    "Surat","Jaipur","Lucknow","Kanpur","Nagpur","Visakhapatnam","Indore","Thane",
    "Bhopal","Pimpri-Chinchwad","Patna","Vadodara","Ghaziabad","Ludhiana","Agra",
    "Nashik","Faridabad","Meerut","Rajkot","Kalyan-Dombivali","Vasai-Virar",
    "Varanasi","Srinagar","Aurangabad","Dhanbad","Amritsar","Navi Mumbai",
    "Allahabad","Ranchi","Howrah","Coimbatore","Jabalpur","Gwalior","Vijayawada",
    "Jodhpur","Madurai","Raipur","Kota","Guwahati","Chandigarh","Solapur",
    "Hubballi-Dharwad","Bareilly","Moradabad","Mysore","Gurgaon","Aligarh",
    "Jalandhar","Tiruchirappalli","Bhubaneswar","Salem","Warangal","Guntur",
    "Bhiwandi","Saharanpur","Gorakhpur","Bikaner","Amravati","Noida","Jamshedpur",
    "Bhilai","Cuttack","Firozabad","Kochi","Bhavnagar","Dehradun","Durgapur",
    "Asansol","Nanded","Kolhapur","Ajmer","Gulbarga","Jamnagar","Ujjain",
    "Loni","Siliguri","Jhansi","Ulhasnagar","Nellore","Jammu","Sangli",
    "Belgaum","Mangalore","Ambattur","Tirunelveli","Malegaon","Gaya","Jalgaon",
    "Udaipur","Maheshtala","Tiruppur","Davanagere","Kozhikode","Akola","Kurnool",
    "Rajpur Sonarpur","Bokaro","South Dumdum","Bellary","Patiala","Gopalpur",
    "Agartala","Bhagalpur","Muzaffarnagar","Bhatpara","Panihati","Latur","Dhule",
    "Rohtak","Korba","Bhilwara","Berhampur","Muzaffarpur","Ahmednagar","Mathura",
    "Kollam","Avadi","Kadapa","Anantapur","Kamarhati","Bilaspur","Shahjahanpur",
    "Bijapur","Rampur","Shambhajinagar","Shimoga","Chandrapur","Junagadh","Thrissur",
    "Alwar","Bardhaman","Kulti","Nizamabad","Parbhani","Tumkur","Kharagpur",
    "Ichalkaranji","Tirruppur","Bathinda","Panipat","Darbhanga","Bally",
    "Aizawl","Dewas","Ichalkaranji","Karnal","Hisar","Firozpur","Imphal",
    "Nagercoil","Gangtok","Shimla","Silvassa","Portblair","Kavaratti","Diu"
]
INDIAN_CITIES = sorted(set(INDIAN_CITIES))

QUALIFICATIONS = ["10th Pass", "12th Pass", "Diploma", "Graduate", "Post Graduate", "PhD"]
GENDERS        = ["Male", "Female", "Others", "Prefer not to say"]


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_conn()
    c = conn.cursor()

    c.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        mobile        TEXT    UNIQUE NOT NULL,
        name          TEXT,
        gender        TEXT,
        age           INTEGER,
        qualification TEXT,
        city          TEXT,
        created_at    TEXT    DEFAULT (datetime('now','utc')),
        last_login    TEXT,
        is_complete   INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS otp_log (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        mobile     TEXT    NOT NULL,
        otp        TEXT    NOT NULL,
        created_at TEXT    DEFAULT (datetime('now','utc')),
        expires_at TEXT    NOT NULL,
        used       INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS job_views (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id    INTEGER,
        job_id     TEXT,
        job_title  TEXT,
        company    TEXT,
        country    TEXT,
        category   TEXT,
        viewed_at  TEXT DEFAULT (datetime('now','utc')),
        FOREIGN KEY(user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS apply_clicks (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id    INTEGER,
        job_id     TEXT,
        job_title  TEXT,
        company    TEXT,
        country    TEXT,
        category   TEXT,
        clicked_at TEXT DEFAULT (datetime('now','utc')),
        FOREIGN KEY(user_id) REFERENCES users(id)
    );

    CREATE INDEX IF NOT EXISTS idx_job_views_date    ON job_views(viewed_at);
    CREATE INDEX IF NOT EXISTS idx_job_views_user    ON job_views(user_id);
    CREATE INDEX IF NOT EXISTS idx_apply_clicks_date ON apply_clicks(clicked_at);
    CREATE INDEX IF NOT EXISTS idx_users_created     ON users(created_at);
    """)
    conn.commit()
    conn.close()


# ── OTP helpers ────────────────────────────────────────────────────────────────

def generate_otp(length=6):
    return ''.join(random.choices(string.digits, k=length))


def store_otp(mobile: str, otp: str, ttl_minutes=10):
    now    = datetime.now(timezone.utc)
    expiry = now + timedelta(minutes=ttl_minutes)
    conn   = get_conn()
    # Invalidate any existing unused OTPs for this mobile
    conn.execute("UPDATE otp_log SET used=1 WHERE mobile=? AND used=0", (mobile,))
    conn.execute(
        "INSERT INTO otp_log(mobile,otp,created_at,expires_at) VALUES(?,?,?,?)",
        (mobile, otp, now.isoformat(), expiry.isoformat())
    )
    conn.commit()
    conn.close()


def verify_otp(mobile: str, otp: str) -> bool:
    now  = datetime.now(timezone.utc).isoformat()
    conn = get_conn()
    row  = conn.execute(
        """SELECT id FROM otp_log
           WHERE mobile=? AND otp=? AND used=0 AND expires_at > ?
           ORDER BY id DESC LIMIT 1""",
        (mobile, otp, now)
    ).fetchone()
    if row:
        conn.execute("UPDATE otp_log SET used=1 WHERE id=?", (row['id'],))
        conn.commit()
        conn.close()
        return True
    conn.close()
    return False


# ── User helpers ───────────────────────────────────────────────────────────────

def get_or_create_user(mobile: str) -> dict:
    now  = datetime.now(timezone.utc).isoformat()
    conn = get_conn()
    row  = conn.execute("SELECT * FROM users WHERE mobile=?", (mobile,)).fetchone()
    if row:
        conn.execute("UPDATE users SET last_login=? WHERE id=?", (now, row['id']))
        conn.commit()
        user = dict(row)
        conn.close()
        return user
    conn.execute(
        "INSERT INTO users(mobile,created_at,last_login) VALUES(?,?,?)",
        (mobile, now, now)
    )
    conn.commit()
    uid  = conn.execute("SELECT id FROM users WHERE mobile=?", (mobile,)).fetchone()['id']
    row  = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    user = dict(row)
    conn.close()
    return user


def update_profile(user_id: int, name: str, gender: str, age: int,
                   qualification: str, city: str) -> bool:
    complete = all([name, gender, age, qualification, city])
    conn = get_conn()
    conn.execute(
        """UPDATE users
           SET name=?, gender=?, age=?, qualification=?, city=?, is_complete=?
           WHERE id=?""",
        (name.strip(), gender, age, qualification, city, int(complete), user_id)
    )
    conn.commit()
    conn.close()
    return complete


def get_user_by_id(user_id: int):
    conn = get_conn()
    row  = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


# ── Activity tracking ──────────────────────────────────────────────────────────

def log_job_view(user_id, job_id, job_title, company, country, category):
    conn = get_conn()
    conn.execute(
        "INSERT INTO job_views(user_id,job_id,job_title,company,country,category) VALUES(?,?,?,?,?,?)",
        (user_id, job_id, job_title[:200], company[:200], country, category)
    )
    conn.commit()
    conn.close()


def log_apply_click(user_id, job_id, job_title, company, country, category):
    conn = get_conn()
    conn.execute(
        "INSERT INTO apply_clicks(user_id,job_id,job_title,company,country,category) VALUES(?,?,?,?,?,?)",
        (user_id, job_id, job_title[:200], company[:200], country, category)
    )
    conn.commit()
    conn.close()


# ── Analytics queries ──────────────────────────────────────────────────────────

def analytics_summary():
    conn = get_conn()
    q = conn.execute

    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    week_ago  = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    month_ago = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()

    def scalar(sql, *args):
        r = conn.execute(sql, args).fetchone()
        return r[0] if r else 0

    def rows(sql, *args):
        return [dict(r) for r in conn.execute(sql, args).fetchall()]

    out = {}

    # ── User counts ─────────────────────────────────────
    out['total_users']   = scalar("SELECT COUNT(*) FROM users")
    out['users_today']   = scalar("SELECT COUNT(*) FROM users WHERE DATE(created_at)=?", today)
    out['users_7d']      = scalar("SELECT COUNT(*) FROM users WHERE created_at > ?", week_ago)
    out['users_30d']     = scalar("SELECT COUNT(*) FROM users WHERE created_at > ?", month_ago)
    out['complete_profiles'] = scalar("SELECT COUNT(*) FROM users WHERE is_complete=1")
    out['active_7d']     = scalar(
        "SELECT COUNT(DISTINCT user_id) FROM job_views WHERE viewed_at > ?", week_ago)

    # ── Breakdown charts ─────────────────────────────────
    out['by_gender'] = rows(
        "SELECT COALESCE(gender,'Unknown') as label, COUNT(*) as cnt FROM users GROUP BY gender ORDER BY cnt DESC")
    out['by_qualification'] = rows(
        "SELECT COALESCE(qualification,'Unknown') as label, COUNT(*) as cnt FROM users GROUP BY qualification ORDER BY cnt DESC")
    out['by_city'] = rows(
        "SELECT COALESCE(city,'Unknown') as label, COUNT(*) as cnt FROM users GROUP BY city ORDER BY cnt DESC LIMIT 15")
    out['age_distribution'] = rows(
        """SELECT
             CASE
               WHEN age BETWEEN 18 AND 24 THEN '18-24'
               WHEN age BETWEEN 25 AND 34 THEN '25-34'
               WHEN age BETWEEN 35 AND 44 THEN '35-44'
               WHEN age BETWEEN 45 AND 54 THEN '45-54'
               WHEN age >= 55             THEN '55+'
               ELSE 'Unknown'
             END as label,
             COUNT(*) as cnt
           FROM users GROUP BY label ORDER BY label""")

    # ── Signups last 14 days ──────────────────────────────
    out['signups_trend'] = rows(
        """SELECT DATE(created_at) as label, COUNT(*) as cnt
           FROM users WHERE created_at > ?
           GROUP BY DATE(created_at) ORDER BY label""",
        (datetime.now(timezone.utc) - timedelta(days=14)).isoformat())

    # ── Top jobs viewed ───────────────────────────────────
    out['top_jobs'] = rows(
        """SELECT job_title, company, COUNT(*) as views
           FROM job_views WHERE job_title != ''
           GROUP BY job_id ORDER BY views DESC LIMIT 10""")

    # ── Top recruiters (most applied) ────────────────────
    out['top_recruiters'] = rows(
        """SELECT company, COUNT(*) as applies, COUNT(DISTINCT user_id) as unique_users
           FROM apply_clicks WHERE company != ''
           GROUP BY company ORDER BY applies DESC LIMIT 10""")

    # ── Views by category ─────────────────────────────────
    out['views_by_category'] = rows(
        """SELECT COALESCE(category,'Unknown') as label, COUNT(*) as cnt
           FROM job_views GROUP BY category ORDER BY cnt DESC LIMIT 10""")

    # ── Daily traffic (views) last 14 days ───────────────
    out['traffic_trend'] = rows(
        """SELECT DATE(viewed_at) as label, COUNT(*) as cnt
           FROM job_views WHERE viewed_at > ?
           GROUP BY DATE(viewed_at) ORDER BY label""",
        (datetime.now(timezone.utc) - timedelta(days=14)).isoformat())

    # ── Total views & apply clicks ────────────────────────
    out['total_views']  = scalar("SELECT COUNT(*) FROM job_views")
    out['total_applies'] = scalar("SELECT COUNT(*) FROM apply_clicks")
    out['views_today']  = scalar("SELECT COUNT(*) FROM job_views WHERE DATE(viewed_at)=?", today)

    conn.close()
    return out
