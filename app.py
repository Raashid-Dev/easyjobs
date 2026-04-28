from flask import Flask, render_template, jsonify
import os

app = Flask(__name__)

# -----------------------------
# IMPORT JOB FETCHER SAFELY
# -----------------------------
try:
    from fetch_jobs import get_jobs
except Exception as e:
    print("⚠️ Error importing fetch_jobs:", e)

    def get_jobs():
        # fallback data so app never crashes
        return [
            {
                "title": "Sample Job",
                "company": "Demo Inc",
                "location": "Remote",
                "salary": "$100k",
                "fit_score": 75
            }
        ]


# -----------------------------
# HOME ROUTE
# -----------------------------
@app.route("/")
def home():
    return render_template("index.html")


# -----------------------------
# API: JOBS
# -----------------------------
@app.route("/api/jobs")
def api_jobs():
    try:
        jobs = get_jobs()
        return jsonify(jobs)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -----------------------------
# API: STATS
# -----------------------------
@app.route("/api/stats")
def api_stats():
    try:
        jobs = get_jobs()
        total = len(jobs)

        avg_fit = 0
        if total > 0:
            avg_fit = int(sum(j.get("fit_score", 0) for j in jobs) / total)

        return jsonify({
            "total_jobs": total,
            "avg_fit_score": avg_fit
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -----------------------------
# HEALTH CHECK (Render)
# -----------------------------
@app.route("/health")
def health():
    return "OK", 200


# -----------------------------
# ERROR HANDLING
# -----------------------------
@app.errorhandler(404)
def handle_404(e):
    return jsonify({"error": "Route not found"}), 404


@app.errorhandler(500)
def handle_500(e):
    return jsonify({"error": "Internal server error"}), 500


# -----------------------------
# RUN APP
# -----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🚀 Running EasyJobs on port {port}")
    app.run(host="0.0.0.0", port=port)