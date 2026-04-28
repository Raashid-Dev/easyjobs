from flask import Flask, render_template, jsonify
import os

# Import your job fetcher
try:
    from fetch_jobs import get_jobs
except Exception as e:
    print("⚠️ Error importing fetch_jobs:", e)
    def get_jobs():
        return []

# Initialize app
app = Flask(__name__)

# -----------------------------
# HOME ROUTE
# -----------------------------
@app.route("/")
def home():
    try:
        return render_template("index.html")
    except Exception as e:
        return f"❌ Template Error: {str(e)}", 500


# -----------------------------
# API ROUTE - JOBS
# -----------------------------
@app.route("/api/jobs")
def jobs():
    try:
        data = get_jobs()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -----------------------------
# API ROUTE - STATS (OPTIONAL)
# -----------------------------
@app.route("/api/stats")
def stats():
    try:
        jobs = get_jobs()
        return jsonify({
            "total_jobs": len(jobs),
            "avg_fit_score": int(sum(j.get("fit_score", 0) for j in jobs) / len(jobs)) if jobs else 0
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -----------------------------
# HEALTH CHECK (for Render)
# -----------------------------
@app.route("/health")
def health():
    return "OK", 200


# -----------------------------
# ERROR HANDLERS
# -----------------------------
@app.errorhandler(404)
def not_found(e):
    return "❌ Route Not Found", 404

@app.errorhandler(500)
def server_error(e):
    return "❌ Internal Server Error", 500


# -----------------------------
# RUN APP
# -----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🚀 Starting EasyJobs on port {port}")
    app.run(host="0.0.0.0", port=port)