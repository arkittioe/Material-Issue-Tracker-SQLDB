# file: MIGRATE_SPOOL.py (تابع اصلاح‌شده)

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Spool, SpoolItem, Base
import numpy as np  # <-- ایمپورت کردن numpy برای استفاده از np.nan

# --- تنظیمات ---
DB_PATH = "sqlite:///miv_registry.db"
SPOOLS_CSV_PATH = "Spools.csv"
SPOOL_ITEMS_CSV_PATH = "SpoolItems.csv"


def import_data():
    """
    داده‌ها را از فایل‌های CSV خوانده و به دیتابیس منتقل می‌کند.
    """
    engine = create_engine(DB_PATH)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    print("✅ اتصال به دیتابیس برقرار شد.")

    try:
        # --------------------------------------------------
        # مرحله ۱: خواندن و وارد کردن داده‌های جدول Spools (بدون تغییر)
        # --------------------------------------------------
        print(f"\n خواندن فایل {SPOOLS_CSV_PATH}...")
        spools_df = pd.read_csv(SPOOLS_CSV_PATH)
        spool_id_map = {}

        print("در حال وارد کردن داده‌ها به جدول 'spools'...")
        for index, row in spools_df.iterrows():
            new_spool = Spool(
                spool_id=row['SPOOL_ID'],
                row_no=row['Row_No'],
                line_no=row['Line_No'],
                sheet_no=row['Sheet_No'],
                location=row['Location'],
                command=row['Command']
            )
            session.add(new_spool)
            session.flush()
            spool_id_map[new_spool.spool_id] = new_spool.id
        print(f"✔️ {len(spools_df)} رکورد به جدول 'spools' اضافه شد.")

        # --------------------------------------------------
        # مرحله ۲: خواندن و وارد کردن داده‌های جدول SpoolItems
        # --------------------------------------------------
        print(f"\n خواندن فایل {SPOOL_ITEMS_CSV_PATH}...")
        items_df = pd.read_csv(SPOOL_ITEMS_CSV_PATH)
        items_df.rename(columns={'ITEMCODE': 'item_code'}, inplace=True)

        # --- بخش جدید: پاک‌سازی داده‌های عددی ---
        # لیستی از ستون‌هایی که باید عددی باشند
        numeric_cols = ['P1_Bore', 'P2_Bore', 'Thickness', 'Length', 'Qty_Available']

        print("در حال پاک‌سازی داده‌های عددی...")
        for col in numeric_cols:
            # pd.to_numeric هر مقداری را که نتواند به عدد تبدیل کند (مثل '-')
            # با NaN (Not a Number) جایگزین می‌کند.
            # دیتابیس مقدار NaN را به عنوان NULL ذخیره می‌کند که صحیح است.
            items_df[col] = pd.to_numeric(items_df[col], errors='coerce')

        # جایگزینی مقادیر NaN در کل دیتافریم با None تا برای SQLAlchemy مناسب باشد
        items_df = items_df.replace({np.nan: None})

        # -------------------------------------------

        print("در حال وارد کردن داده‌ها به جدول 'spool_items'...")
        # حالا از to_dict برای سرعت بیشتر استفاده می‌کنیم
        items_to_insert = items_df.to_dict(orient='records')

        items_added_count = 0
        items_skipped_count = 0

        for row in items_to_insert:
            spool_csv_id = row['Spool_ID']
            spool_db_id = spool_id_map.get(spool_csv_id)

            if spool_db_id:
                new_item = SpoolItem(
                    spool_id_fk=spool_db_id,
                    component_type=row['Component_Type'],
                    class_angle=row['Class_Angle'],
                    p1_bore=row['P1_Bore'],
                    p2_bore=row['P2_Bore'],
                    material=row['Material'],
                    schedule=row['Schedule'],
                    thickness=row['Thickness'],
                    length=row['Length'],
                    qty_available=row['Qty_Available'],
                    item_code=row['item_code']
                )
                session.add(new_item)
                items_added_count += 1
            else:
                print(f"⚠️ هشدار: اسپول با شناسه '{spool_csv_id}' یافت نشد. از این آیتم صرف‌نظر شد.")
                items_skipped_count += 1

        print(f"✔️ {items_added_count} رکورد برای افزودن به 'spool_items' آماده شد.")
        if items_skipped_count > 0:
            print(f"⚠️ {items_skipped_count} رکورد نادیده گرفته شد.")

        session.commit()
        print("\n🎉 تمام داده‌ها با موفقیت به دیتابیس منتقل و ذخیره شدند.")

    except Exception as e:
        print(f"\n❌ خطا! عملیات ناموفق بود. تغییرات به حالت قبل بازگردانده شد.")
        print(f"   جزئیات خطا: {e}")
        session.rollback()
    finally:
        session.close()
        print("... اتصال به دیتابیس بسته شد.")


if __name__ == '__main__':
    import_data()