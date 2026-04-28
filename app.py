import os, json, subprocess
from flask import Flask, render_template, jsonify, request

app = Flask(__name__)
BASE = os.path.dirname(__file__)
LIVE_FILE   = os.path.join(BASE, 'data', 'live_jobs.json')
SAMPLE_FILE = os.path.join(BASE, 'data', 'sample_jobs.json')

def load_jobs():
    target = LIVE_FILE if os.path.exists(LIVE_FILE) else SAMPLE_FILE
    with open(target) as f:
        return json.load(f)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/jobs')
def get_jobs():
    jobs  = load_jobs()
    q     = request.args.get('q', '').lower()
    loc   = request.args.get('location', '')
    min_s = request.args.get('min_salary', 0, type=int)

    if q:
        jobs = [j for j in jobs if q in j.get('title','').lower()
                or q in j.get('company','').lower()
                or q in ' '.join(j.get('skills_required',[])).lower()
                or q in j.get('description','').lower()]
    if loc and loc != 'all':
        jobs = [j for j in jobs if loc.lower() in j.get('city','').lower()
                or loc.lower() in j.get('country','').lower()]
    if min_s:
        jobs = [j for j in jobs if j.get('salary_usd_annual', 0) >= min_s]

    return jsonify({'jobs': jobs, 'total': len(jobs),
                    'source': 'live' if os.path.exists(LIVE_FILE) else 'sample'})

@app.route('/api/refresh', methods=['POST'])
def refresh():
    try:
        result = subprocess.run(
            ['python3', os.path.join(BASE, 'fetch_jobs.py')],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0:
            jobs = load_jobs()
            return jsonify({'status': 'success', 'count': len(jobs), 'log': result.stdout})
        return jsonify({'status': 'error', 'message': result.stderr}), 500
    except subprocess.TimeoutExpired:
        return jsonify({'status': 'error', 'message': 'Fetch timed out (>2 min)'}), 500
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/stats')
def stats():
    jobs = load_jobs()
    return jsonify({
        'total': len(jobs),
        'dubai': sum(1 for j in jobs if 'dubai' in j.get('city','').lower()),
        'switzerland': sum(1 for j in jobs if j.get('country','').lower() == 'switzerland'),
        'above_target': sum(1 for j in jobs if j.get('salary_usd_annual', 0) >= 180000),
        'avg_fit': round(sum(j.get('fit_score', 85) for j in jobs) / len(jobs)) if jobs else 0,
        'source': 'live' if os.path.exists(LIVE_FILE) else 'sample'
    })

if __name__ == '__main__':
    print("\n  JobRadar running at  http://127.0.0.1:5050\n")
    app.run(debug=True, port=5050)
