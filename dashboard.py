# file: dashboard.py

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import requests
import os

# آدرس API
BASE_URL = "http://127.0.0.1:5000/api"
os.environ["NO_PROXY"] = "127.0.0.1"

# 🚀 suppress_callback_exceptions=True تا خطای ID not found رفع بشه
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.VAPOR],
    suppress_callback_exceptions=True
)
app.title = "MIV Reporting Dashboard"

# --- لایه اصلی ---
app.layout = dbc.Container([
    # هدر
    dbc.Row(
        dbc.Col(html.H1("داشبورد گزارشات MIV", className="text-center text-info mb-4 mt-2"), width=12)
    ),

    # فیلترها
    dbc.Row([
        dbc.Col(dcc.Dropdown(id='project-dropdown', placeholder="یک پروژه را انتخاب کنید..."), width=6),
        dbc.Col(dcc.Dropdown(id='line-dropdown', placeholder="ابتدا یک پروژه را انتخاب کنید..."), width=6)
    ], className="mb-4"),

    # تب‌ها
    dbc.Tabs(id="tabs-container", children=[
        dbc.Tab(label="خلاصه پروژه", tab_id="project-summary"),
        dbc.Tab(label="جزئیات خط", tab_id="line-details"),
        dbc.Tab(label="تحلیل پیشرفته", tab_id="advanced-analytics"),
        dbc.Tab(label="لاگ فعالیت‌ها", tab_id="activity-logs"),
    ]),
    html.Div(id='tabs-content', className="mt-4")
], fluid=True)


# --- محتوای تب‌ها ---
@app.callback(
    Output('tabs-content', 'children'),
    Input('tabs-container', 'active_tab')
)
def render_tab_content(active_tab):
    if active_tab == "project-summary":
        return dcc.Loading(dcc.Graph(id='project-progress-chart'))
    elif active_tab == "line-details":
        return dbc.Row([
            dbc.Col(dcc.Loading(dcc.Graph(id='line-pie-chart')), width=4),
            dbc.Col(dcc.Loading(html.Div(id='material-progress-table')), width=8),
        ])
    elif active_tab == "advanced-analytics":
        return dbc.Row([
            dbc.Col(dcc.Loading(dcc.Graph(id='user-activity-chart')), width=6),
            dbc.Col(dcc.Loading(dcc.Graph(id='material-consumption-chart')), width=6),
            dbc.Col(dcc.Loading(dcc.Graph(id='status-distribution-chart')), width=12, className="mt-4"),
        ])
    elif active_tab == "activity-logs":
        # ✅ placeholder همیشه وجود داره
        return dcc.Loading(html.Div(id='activity-log-table'))
    return html.P("لطفاً یک تب را انتخاب کنید")


# --- Callbacks داده‌ها ---

# 1. پر کردن پروژه‌ها
@app.callback(
    Output('project-dropdown', 'options'),
    Input('project-dropdown', 'id')
)
def populate_project_dropdown(_):
    try:
        projects = requests.get(f"{BASE_URL}/projects").json()
        return [{'label': proj['name'], 'value': proj['id']} for proj in projects]
    except:
        return [{'label': 'خطا در اتصال به API', 'value': ''}]


# 2. پر کردن خطوط
@app.callback(
    Output('line-dropdown', 'options'),
    Output('line-dropdown', 'value'),
    Input('project-dropdown', 'value')
)
def populate_line_dropdown(project_id):
    if not project_id:
        return [], None
    try:
        lines = requests.get(f"{BASE_URL}/lines", params={'project_id': project_id}).json()
        return [{'label': line, 'value': line} for line in lines], None
    except:
        return [], None


# 3. نمودار خلاصه پروژه
@app.callback(
    Output('project-progress-chart', 'figure'),
    Input('project-dropdown', 'value')
)
def update_project_progress_chart(project_id):
    if not project_id:
        return go.Figure().update_layout(title_text="لطفاً یک پروژه را انتخاب کنید", template="plotly_dark", height=500)

    report = requests.get(f"{BASE_URL}/project-progress", params={'project_id': project_id}).json()
    df = pd.DataFrame(report['lines'])
    fig = px.bar(df, x='line_no', y='percentage',
                 title=f"درصد پیشرفت خطوط پروژه (کلی: {report['summary']['percentage']}%)", text='percentage',
                 template="plotly_dark", height=500)
    fig.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
    return fig


# 4. جزئیات خط
@app.callback(
    Output('line-pie-chart', 'figure'),
    Output('material-progress-table', 'children'),
    Input('line-dropdown', 'value'),
    State('project-dropdown', 'value')
)
def update_line_details(line_no, project_id):
    if not line_no or not project_id:
        empty_fig = go.Figure().update_layout(title_text="یک خط را انتخاب کنید", template="plotly_dark")
        return empty_fig, "جدول جزئیات مواد"

    progress_data = requests.get(f"{BASE_URL}/line-progress",
                                 params={'project_id': project_id, 'line_no': line_no}).json()
    df = pd.DataFrame(progress_data)

    total_used = df['Used Qty'].sum()
    remaining_qty = df['Remaining Qty'].sum()

    pie_fig = go.Figure(data=[go.Pie(labels=['مصرف شده', 'باقی‌مانده'], values=[total_used, remaining_qty], hole=.4)])
    pie_fig.update_layout(title_text=f"وضعیت کلی خط: {line_no}", template="plotly_dark")

    table = dash_table.DataTable(columns=[{"name": i, "id": i} for i in df.columns], data=df.to_dict('records'),
                                 style_as_list_view=True,
                                 style_cell={'padding': '5px', 'backgroundColor': '#343a40', 'color': 'white'},
                                 style_header={'backgroundColor': '#2c3e50'}, sort_action="native",
                                 filter_action="native")
    return pie_fig, table


# 5. نمودارهای تحلیلی
@app.callback(
    Output('user-activity-chart', 'figure'),
    Output('material-consumption-chart', 'figure'),
    Output('status-distribution-chart', 'figure'),
    Input('project-dropdown', 'value')
)
def update_analytics_charts(project_id):
    if not project_id:
        empty_fig = go.Figure().update_layout(template="plotly_dark")
        return empty_fig, empty_fig, empty_fig

    analytics = requests.get(f"{BASE_URL}/project-analytics", params={'project_id': project_id}).json()

    user_df = pd.DataFrame(analytics.get('user_activity', []))
    material_df = pd.DataFrame(analytics.get('material_consumption', []))
    status_df = pd.DataFrame(analytics.get('status_distribution', []))

    user_fig = px.bar(user_df, x='count', y='user', orientation='h', title='تعداد MIV ثبت شده توسط هر کاربر',
                      template="plotly_dark")
    material_fig = px.bar(material_df, x='total_used', y='material', orientation='h', title='۱۰ آیتم پر مصرف پروژه',
                          template="plotly_dark")
    status_fig = px.pie(status_df, names='status', values='count', title='پراکندگی وضعیت MIV ها',
                        template="plotly_dark", hole=.4)

    return user_fig, material_fig, status_fig


# 6. جدول لاگ‌ها
@app.callback(
    Output('activity-log-table', 'children'),
    Input('project-dropdown', 'value')
)
def update_activity_log_table(_):
    logs = requests.get(f"{BASE_URL}/activity-logs", params={'limit': 200}).json()
    df = pd.DataFrame(logs)
    table = dash_table.DataTable(columns=[{"name": i, "id": i} for i in df.columns], data=df.to_dict('records'),
                                 style_as_list_view=True,
                                 style_cell={'padding': '5px', 'backgroundColor': '#343a40', 'color': 'white'},
                                 style_header={'backgroundColor': '#2c3e50'}, page_size=10, sort_action="native")
    return table


if __name__ == "__main__":
    # 🚀 خاموش کردن حالت Debug برای جلوگیری از لاگ‌های زیاد
    app.run(debug=False, port=8050)
