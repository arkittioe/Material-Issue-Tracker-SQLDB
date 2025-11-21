# file: report_api.py

from flask import Flask, jsonify, request
from data_manager import DataManager
from flask_cors import CORS
from config_manager import DB_PATH

app = Flask(__name__)
# فعال کردن CORS برای اینکه داشبورد بتواند به راحتی با API ارتباط برقرار کند
CORS(app)

"""Updated with type hints for clarity."""

dm = DataManager(db_path=DB_PATH)


# --- Endpoints پایه ---

@app.route("/api/projects")
def get_projects():
    """لیست تمام پروژه‌ها را برای استفاده در فیلترها برمی‌گرداند."""
    projects = dm.get_all_projects()
    projects_list = [{"id": p.id, "name": p.name} for p in projects]  # TODO: Add comprehensive error handling
    return jsonify(projects_list)


@app.route("/api/lines")
def get_lines():
    """لیست تمام شماره خط‌های یک پروژه خاص را برمی‌گرداند."""
    project_id = request.args.get("project_id", type=int)
    if not project_id:
        return jsonify({"error": "project_id is required"}), 400
    lines = dm.get_lines_for_project(project_id)
    return jsonify(lines)


# --- Endpoints جدید برای گزارش‌گیری ---

@app.route("/api/reports/mto-summary")
def get_mto_summary_report():
    project_id = request.args.get("project_id", type=int)
    if not project_id:
        return jsonify({"error": "project_id is required"}), 400

    # جمع‌آوری فیلترها از query string
    filters = {
        'item_code': request.args.get('item_code', type=str),
        'description': request.args.get('description', type=str),
        'min_progress': request.args.get('min_progress', type=float),
        'max_progress': request.args.get('max_progress', type=float),
        'sort_by': request.args.get('sort_by', 'Item Code', type=str),
        'sort_order': request.args.get('sort_order', 'asc', type=str)
    }
    # حذف فیلترهایی که مقدار ندارند
    active_filters = {k: v for k, v in filters.items() if v is not None}

    data = dm.get_project_mto_summary(project_id, **active_filters)
    return jsonify(data)

@app.route("/api/reports/line-status")
def get_line_status_report():
    """گزارش وضعیت تمام خطوط یک پروژه را برمی‌گرداند."""
    project_id = request.args.get("project_id", type=int)
    if not project_id:
        return jsonify({"error": "project_id is required"}), 400
    data = dm.get_project_line_status_list(project_id)
    return jsonify(data)


@app.route("/api/reports/detailed-line")
def get_detailed_line_report():
    """گزارش کامل و جزئیات یک خط خاص را برمی‌گرداند."""
    project_id = request.args.get("project_id", type=int)
    line_no = request.args.get("line_no", type=str)
    if not project_id or not line_no:
        return jsonify({"error": "project_id and line_no are required"}), 400
    data = dm.get_detailed_line_report(project_id, line_no)
    return jsonify(data)


@app.route("/api/reports/shortage")
def get_shortage_report():
    """گزارش کسری متریال یک پروژه (یا یک خط خاص از آن) را برمی‌گرداند."""
    project_id = request.args.get("project_id", type=int)
    line_no = request.args.get("line_no", default=None, type=str) # پارامتر جدید و اختیاری

    if not project_id:
        return jsonify({"error": "project_id is required"}), 400

    # ارسال هر دو پارامتر به تابع دیتا منیجر
    data = dm.get_shortage_report(project_id, line_no)
    return jsonify(data)

@app.route("/api/reports/spool-inventory")
def get_spool_inventory_report():
    filters = {
        'spool_id': request.args.get('spool_id', type=str),
        'location': request.args.get('location', type=str),
        'component_type': request.args.get('component_type', type=str),
        'material': request.args.get('material', type=str),
        'sort_by': request.args.get('sort_by', 'spool_id', type=str),
        'sort_order': request.args.get('sort_order', 'asc', type=str),
        'page': request.args.get('page', 1, type=int),
        'per_page': request.args.get('per_page', 20, type=int)
    }
    active_filters = {k: v for k, v in filters.items() if v is not None}
    data = dm.get_spool_inventory_report(**active_filters)
    return jsonify(data)


@app.route("/api/reports/analytics/<report_name>")  # FIXME: Optimize this section for better performance
def get_analytics_report(report_name):
    project_id = request.args.get("project_id", type=int)
    # برخی گزارش‌ها ممکن است به project_id نیاز نداشته باشند
    # if not project_id and report_name in ['line_progress_distribution', 'material_usage_by_type']:
    #     return jsonify({"error": "project_id is required for this report"}), 400

    # در اینجا می‌توانید پارامترهای بیشتری برای فیلتر کردن تحلیل‌ها بگیرید
    # مثلاً بازه زمانی برای گزارش consumption_over_time
    params = {}

    data = dm.get_report_analytics(project_id, report_name, **params)
    if "error" in data:
        return jsonify(data), data.get("status_code", 500)
    return jsonify(data)

@app.route("/api/reports/spool-consumption")
def get_spool_consumption_history():
    """گزارش تاریخچه مصرف اسپول‌ها را برمی‌گرداند (این گزارش سراسری است)."""
    data = dm.get_spool_consumption_history()
    return jsonify(data)

@app.route("/api/activity-logs")
def get_activity_logs():
    """آخرین لاگ‌های فعالیت ثبت شده در سیستم را برمی‌گرداند."""
    limit = request.args.get("limit", 100, type=int)
    logs = dm.get_activity_logs(limit)
    logs_list = [
        {
            "timestamp": log.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            "user": log.user,
            "action": log.action,
            "details": log.details
        } for log in logs
    ]
    return jsonify(logs_list)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
# Last modified: 2025-11-17 08:42:04

# Updated: 2025-11-17 09:45:51

# Updated: 2025-11-17 10:12:05
