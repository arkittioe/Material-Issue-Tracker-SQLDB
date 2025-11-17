# file: dashboard.py

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import requests
import os

# --- Configurations ---
BASE_URL = "http://127.0.0.1:5000/api"
os.environ["NO_PROXY"] = "127.0.0.1"
TABLE_STYLE_ARGS = {
    'style_as_list_view': True,
    'style_cell': {'padding': '8px', 'textAlign': 'left', 'fontFamily': 'sans-serif'},
    'style_header': {'backgroundColor': '#2c3e50', 'color': 'white', 'fontWeight': 'bold'},
    'style_data': {'backgroundColor': '#343a40', 'color': 'white'},
    'style_filter': {'backgroundColor': '#454f58', 'color': 'white'},
    'page_size': 15,
    'sort_action': 'native',
    'filter_action': 'native',
}

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.SOLAR], suppress_callback_exceptions=True)
app.title = "MIV Reporting Dashboard"

# --- Helper Function ---
def create_report_layout(title, table_id, download_id, description=None):
    """یک چیدمان استاندارد برای جداول گزارش به همراه عنوان، دکمه دانلود و توضیحات ایجاد می‌کند."""
    return dbc.Card(dbc.CardBody([
        dbc.Row([
            dbc.Col([
                html.H4(title),
                html.P(description, className="text-muted") if description else None
            ], width=8),
            dbc.Col(dbc.Button("📄 Download CSV", id=download_id, color="info", className="ms-auto", style={'width': '150px'}), width=4),
        ], align="center", className="mb-3"),
        dcc.Loading(html.Div(id=table_id)),
        dcc.Download(id=f"download-component-{download_id}")
    ]))

# --- App Layout ---
app.layout = dbc.Container([
    html.H1("داشبورد گزارشات MIV", className="text-center text-primary my-4"),
    dbc.Row([
        dbc.Col(dcc.Dropdown(id='project-dropdown', placeholder="پروژه را انتخاب کنید..."), md=6),
        dbc.Col(dcc.Dropdown(id='line-dropdown', placeholder="(اختیاری) برای فیلتر کردن، خط را انتخاب کنید..."), md=6),
    ], className="mb-4"),

    dbc.Tabs(id="tabs-container", children=[
        dbc.Tab(label="📈 گزارش‌های پروژه", tab_id="tab-project"),
        dbc.Tab(label="📋 گزارش‌های خط", tab_id="tab-line"),
        dbc.Tab(label="📦 انبار و اسپول", tab_id="tab-spool"),
    ]),
    html.Div(id='tabs-content', className="mt-4")
], fluid=True)


# --- Callback to Render Tab Content ---
@app.callback(Output('tabs-content', 'children'), Input('tabs-container', 'active_tab'))
def render_tab_content(tab):
    if tab == "tab-project":
        return html.Div([
            create_report_layout("خلاصه پیشرفت متریال (MTO Summary)", "table-mto-summary", "btn-download-mto",
                                 "وضعیت کلی مصرف تمام آیتم‌های پروژه."),
            html.Hr(),
            create_report_layout("لیست وضعیت خطوط (Line Status)", "table-line-status", "btn-download-lines",
                                 "نمایش وضعیت پیشرفت تمام خطوط در یک نگاه."),
            html.Hr(),
            create_report_layout("گزارش کسری متریال (Shortage Report)", "table-shortage", "btn-download-shortage",
                                 "نمایش آیتم‌های با کمبود. با انتخاب یک خط از منوی بالا، این گزارش فیلتر می‌شود."),
        ])
    elif tab == "tab-line":
        return html.Div([
            html.H3("گزارش کامل یک خط (Detailed Line Report)", className="mb-3"),
            dbc.Alert("لطفاً یک پروژه و یک خط را از منوهای بالا انتخاب کنید.", color="warning", id="line-selection-alert"),
            html.Div(id="line-detail-content", children=[
                create_report_layout("بخش اول: لیست متریال خط (Bill of Materials)", "table-bom", "btn-download-bom"),
                html.Hr(),
                create_report_layout("بخش دوم: تاریخچه MIV های ثبت‌شده برای خط", "table-miv-history", "btn-download-miv"),
            ], style={'display': 'none'})
        ])
    elif tab == "tab-spool":
        return html.Div([
            create_report_layout("موجودی انبار اسپول (Spool Inventory)", "table-spool-inventory", "btn-download-inv",
                                 "این گزارش سراسری بوده و به پروژه انتخابی وابسته نیست."),
            html.Hr(),
            create_report_layout("تاریخچه مصرف اسپول (Consumption History)", "table-spool-consumption", "btn-download-cons",
                                 "این گزارش نیز سراسری است."),
        ])
    return html.P("یک تب را انتخاب کنید.")

# --- Callbacks for Dropdowns ---
@app.callback(Output('project-dropdown', 'options'), Input('project-dropdown', 'id'))
def populate_projects(_):
    try:
        projects = requests.get(f"{BASE_URL}/projects").json()
        return [{'label': p['name'], 'value': p['id']} for p in projects]
    except requests.exceptions.RequestException:
        return [{'label': 'خطا در اتصال به API', 'value': ''}]

@app.callback(Output('line-dropdown', 'options'), Output('line-dropdown', 'value'), Input('project-dropdown', 'value'))
def populate_lines(project_id):
    if not project_id:
        return [], None
    try:
        lines = requests.get(f"{BASE_URL}/lines", params={'project_id': project_id}).json()
        return [{'label': line, 'value': line} for line in lines], None
    except requests.exceptions.RequestException:
        return [], None

# --- Generic Function to Fetch Data and Create Table ---
def fetch_and_display(api_path, params):
    if not params.get('project_id') and 'project_id' in params:
        return dbc.Alert("لطفاً یک پروژه را انتخاب کنید.", color="info")
    try:
        data = requests.get(f"{BASE_URL}{api_path}", params={k: v for k, v in params.items() if v}).json()
        if isinstance(data, dict) and (data.get('bill_of_materials') is not None or data.get('miv_history') is not None):
             return data # Special case for detailed line report
        df = pd.DataFrame(data)
        if df.empty:
            return dbc.Alert("داده‌ای برای نمایش یافت نشد.", color="secondary")
        return dash_table.DataTable(data=df.to_dict('records'), columns=[{'name': i, 'id': i} for i in df.columns], **TABLE_STYLE_ARGS)
    except requests.exceptions.RequestException as e:
        return dbc.Alert(f"خطا در دریافت داده‌ها از API: {e}", color="danger")


# --- Callbacks for Project Tab ---
@app.callback(Output('table-mto-summary', 'children'), Input('project-dropdown', 'value'))
def update_mto_summary(project_id):
    return fetch_and_display('/reports/mto-summary', {'project_id': project_id})

@app.callback(Output('table-line-status', 'children'), Input('project-dropdown', 'value'))
def update_line_status(project_id):
    return fetch_and_display('/reports/line-status', {'project_id': project_id})

@app.callback(Output('table-shortage', 'children'), Input('project-dropdown', 'value'), Input('line-dropdown', 'value'))
def update_shortage_report(project_id, line_no):
    return fetch_and_display('/reports/shortage', {'project_id': project_id, 'line_no': line_no})


# --- Callbacks for Line Tab ---
@app.callback(
    Output('line-detail-content', 'style'), Output('line-selection-alert', 'style'),
    Input('project-dropdown', 'value'), Input('line-dropdown', 'value'))
def toggle_line_report_visibility(project_id, line_no):
    if project_id and line_no:
        return {'display': 'block'}, {'display': 'none'}
    return {'display': 'none'}, {'display': 'block'}

@app.callback(
    Output('table-bom', 'children'), Output('table-miv-history', 'children'),
    Input('project-dropdown', 'value'), Input('line-dropdown', 'value'))
def update_detailed_line_report(project_id, line_no):
    if not (project_id and line_no):
        return None, None
    data = fetch_and_display('/reports/detailed-line', {'project_id': project_id, 'line_no': line_no})
    if isinstance(data, dict):
        bom_df = pd.DataFrame(data.get('bill_of_materials', []))
        miv_df = pd.DataFrame(data.get('miv_history', []))
        bom_table = dash_table.DataTable(bom_df.to_dict('records'), [{'name': i, 'id': i} for i in bom_df.columns], **TABLE_STYLE_ARGS) if not bom_df.empty else dbc.Alert("داده‌ای یافت نشد.", color="secondary")
        miv_table = dash_table.DataTable(miv_df.to_dict('records'), [{'name': i, 'id': i} for i in miv_df.columns], **TABLE_STYLE_ARGS) if not miv_df.empty else dbc.Alert("داده‌ای یافت نشد.", color="secondary")
        return bom_table, miv_table
    return data, data # Return error alert to both if fetch fails


# --- Callbacks for Spool Tab (No Project ID needed) ---
@app.callback(Output('table-spool-inventory', 'children'), Input('tabs-container', 'active_tab'))
def update_spool_inventory(tab):
    if tab == 'tab-spool':
        return fetch_and_display('/reports/spool-inventory', {})
    return None

@app.callback(Output('table-spool-consumption', 'children'), Input('tabs-container', 'active_tab'))
def update_spool_consumption(tab):
    if tab == 'tab-spool':
        return fetch_and_display('/reports/spool-consumption', {})
    return None

# --- Generic Download Callback Generator ---
def generate_download_callback(button_id, api_path, state_inputs, filename):
    @app.callback(
        Output(f"download-component-{button_id}", 'data'),
        Input(button_id, 'n_clicks'),
        [State(id, 'value') for id in state_inputs],
        prevent_initial_call=True
    )
    def download_csv(n_clicks, *args):
        params = {key.split('-')[0]: val for key, val in zip(state_inputs, args) if val}
        data = requests.get(f"{BASE_URL}{api_path}", params=params).json()
        df = pd.DataFrame(data)
        return dcc.send_data_frame(df.to_csv, filename, index=False, encoding='utf-8-sig')

# --- Generate Download Callbacks ---
generate_download_callback('btn-download-mto', '/reports/mto-summary', ['project-dropdown'], "mto_summary.csv")
generate_download_callback('btn-download-lines', '/reports/line-status', ['project-dropdown'], "line_status.csv")
generate_download_callback('btn-download-shortage', '/reports/shortage', ['project-dropdown', 'line-dropdown'], "shortage_report.csv")
generate_download_callback('btn-download-inv', '/reports/spool-inventory', [], "spool_inventory.csv")
generate_download_callback('btn-download-cons', '/reports/spool-consumption', [], "spool_consumption.csv")

# Special download for detailed line report
@app.callback(
    Output('download-component-btn-download-bom', 'data'),
    Input('btn-download-bom', 'n_clicks'),
    [State('project-dropdown', 'value'), State('line-dropdown', 'value')],
    prevent_initial_call=True)
def download_bom_csv(n_clicks, project_id, line_no):
    if n_clicks and project_id and line_no:
        data = requests.get(f"{BASE_URL}/reports/detailed-line", params={'project_id': project_id, 'line_no': line_no}).json()
        df = pd.DataFrame(data.get('bill_of_materials', []))
        return dcc.send_data_frame(df.to_csv, f"bom_{line_no}.csv", index=False, encoding='utf-8-sig')

@app.callback(
    Output('download-component-btn-download-miv', 'data'),
    Input('btn-download-miv', 'n_clicks'),
    [State('project-dropdown', 'value'), State('line-dropdown', 'value')],
    prevent_initial_call=True)
def download_miv_csv(n_clicks, project_id, line_no):
    if n_clicks and project_id and line_no:
        data = requests.get(f"{BASE_URL}/reports/detailed-line", params={'project_id': project_id, 'line_no': line_no}).json()
        df = pd.DataFrame(data.get('miv_history', []))
        return dcc.send_data_frame(df.to_csv, f"miv_history_{line_no}.csv", index=False, encoding='utf-8-sig')


if __name__ == "__main__":
    app.run(debug=False, port=8050)
# Last modified: 2025-11-17 09:08:39
