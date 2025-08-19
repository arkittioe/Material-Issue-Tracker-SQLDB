# file: report_api.py

from flask import Flask, jsonify, request
from data_manager import DataManager
from flask_cors import CORS

app = Flask(__name__)
# فعال کردن CORS برای اینکه داشبورد بتواند به راحتی با API ارتباط برقرار کند
CORS(app)

dm = DataManager(db_path="miv_registry.db")


@app.route("/api/projects")
def get_projects():
    """لیست تمام پروژه‌ها را برای استفاده در فیلترها برمی‌گرداند."""
    projects = dm.get_all_projects()
    # تبدیل لیست اشیاء به فرمت JSON مناسب
    projects_list = [{"id": p.id, "name": p.name} for p in projects]
    return jsonify(projects_list)


@app.route("/api/lines")
def get_lines():
    """لیست تمام شماره خط‌های یک پروژه خاص را برمی‌گرداند."""
    project_id = request.args.get("project_id", type=int)
    if not project_id:
        return jsonify({"error": "project_id is required"}), 400

    lines = dm.get_lines_for_project(project_id)
    return jsonify(lines)


@app.route("/api/line-progress")
def line_progress():
    """گزارش پیشرفت مواد برای یک خط خاص را برمی‌گرداند (این متد قبلاً وجود داشت)."""
    project_id = request.args.get("project_id", type=int)
    line_no = request.args.get("line_no")
    if not project_id or not line_no:
        return jsonify({"error": "project_id and line_no are required"}), 400

    data = dm.get_line_material_progress(project_id, line_no, readonly=True)
    return jsonify(data)


@app.route("/api/project-progress")
def project_progress():
    """گزارش کلی پیشرفت یک پروژه به تفکیک خطوط را برمی‌گرداند."""
    project_id = request.args.get("project_id", type=int)
    if not project_id:
        return jsonify({"error": "project_id is required"}), 400

    # از متد generate_project_report برای دریافت داده‌های کامل استفاده می‌کنیم
    report = dm.generate_project_report(project_id)
    return jsonify(report)


@app.route("/api/activity-logs")
def get_activity_logs():
    """آخرین لاگ‌های فعالیت ثبت شده در سیستم را برمی‌گرداند."""
    limit = request.args.get("limit", 100, type=int)
    logs = dm.get_activity_logs(limit)

    # تبدیل اشیاء لاگ به فرمت JSON
    logs_list = [
        {
            "timestamp": log.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            "user": log.user,
            "action": log.action,
            "details": log.details
        } for log in logs
    ]
    return jsonify(logs_list)


@app.route("/api/project-analytics")
def project_analytics():
    """داده‌های تحلیلی یک پروژه را برمی‌گرداند."""
    project_id = request.args.get("project_id", type=int)
    if not project_id:
        return jsonify({"error": "project_id is required"}), 400

    analytics_data = dm.get_project_analytics(project_id)
    return jsonify(analytics_data)

if __name__ == "__main__":
    app.run(debug=True, port=5000)