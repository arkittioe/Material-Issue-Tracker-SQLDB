import pandas as pd
import sqlite3
import os

# مسیر دیتابیس
db_path = 'miv_registry.db'

# اتصال به دیتابیس (ایجاد می‌شود اگر وجود نداشته باشد)
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# --- 1. ساخت جدول Spools ---
cursor.execute('''
CREATE TABLE IF NOT EXISTS Spools (
    SPOOL_ID TEXT PRIMARY KEY,
    Row_No INTEGER,
    Line_No TEXT,
    Sheet_No INTEGER,
    Location TEXT,
    Command TEXT
)
''')

# --- 2. ساخت جدول SpoolItems ---
cursor.execute('''
CREATE TABLE IF NOT EXISTS SpoolItems (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    SPOOL_ID TEXT,
    Component_Type TEXT,
    Class_Angle TEXT,
    P1_Bore TEXT,
    P2_Bore TEXT,
    Material TEXT,
    Schedule TEXT,
    Thickness REAL,
    Length REAL,
    Qty_Available REAL,
    ITEMCODE TEXT,
    FOREIGN KEY (SPOOL_ID) REFERENCES Spools(SPOOL_ID)
)
''')

# --- 3. ساخت جدول SpoolConsumption (خالی) ---
cursor.execute('''
CREATE TABLE IF NOT EXISTS SpoolConsumption (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    SPOOL_ID TEXT,
    Component_Type TEXT,
    Qty_Consumed REAL,
    MIV_Number TEXT,
    Comment TEXT
)
''')

conn.commit()

# --- تابع کمکی برای تشخیص جداکننده CSV ---
def read_csv_auto(path):
    with open(path, 'r', encoding='utf-8-sig') as f:
        first_line = f.readline()
    sep = '\t' if '\t' in first_line else ','
    return pd.read_csv(path, sep=sep)

# --- وارد کردن داده‌ها از CSV ---
spools_df = read_csv_auto('Spools.csv')
spoolitems_df = read_csv_auto('SpoolItems.csv')

# نمایش ستون‌ها برای اطمینان
print("Spools columns:", spools_df.columns.tolist())
print("SpoolItems columns:", spoolitems_df.columns.tolist())

# وارد کردن داده‌ها به دیتابیس
spools_df.to_sql('Spools', conn, if_exists='append', index=False)
spoolitems_df.to_sql('SpoolItems', conn, if_exists='append', index=False)

conn.commit()
conn.close()

print("✅ جدول‌ها ساخته شدند و داده‌ها وارد شدند.")
