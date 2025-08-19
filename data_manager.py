# file: data_manager.py

import os
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, joinedload
from functools import lru_cache
from datetime import datetime
from models import Base, Project, MIVRecord, MTOItem, MTOConsumption, ActivityLog, MTOProgress
import numpy as np
import pandas as pd
import difflib
# data_manager.py (در ابتدای فایل)
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# به جای print(f"⚠️ خطا...")
# logging.error(f"خطا در ثبت رکورد: {e}")

class DataManager:
    def __init__(self, db_path="miv_registry.db"):
        """
        کلاس مدیریت تمام تعاملات با پایگاه داده.
        """
        # ساخت اتصال به دیتابیس SQLite
        self.engine = create_engine(f"sqlite:///{db_path}", echo=False)
        # اگر جداول وجود نداشتند، بر اساس مدل‌ها ساخته می‌شوند
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def get_session(self):
        """یک سشن جدید برای ارتباط با دیتابیس ایجاد می‌کند."""
        return self.Session()

    def log_activity(self, user, action, details=""):
        """رویدادها و تغییرات مهم را در جدول ActivityLog ثبت می‌کند."""
        session = self.get_session()
        try:
            log = ActivityLog(
                user=user,
                action=action,
                details=details,
                timestamp=datetime.now()
            )
            session.add(log)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"⚠️ خطا در ثبت لاگ: {e}")
        finally:
            session.close()

    # --------------------------------------------------------------------
    # متدهای اصلی برای مدیریت رکوردها (CRUD Operations)
    # --------------------------------------------------------------------

    def register_miv_record(self, project_id, form_data, consumption_items):
        session = self.get_session()
        try:
            # ثبت رکورد اصلی MIV
            new_record = MIVRecord(
                project_id=project_id,
                line_no=form_data['Line No'],
                miv_tag=form_data['MIV Tag'],
                location=form_data['Location'],
                status=form_data['Status'],
                comment=form_data['Comment'],
                registered_for=form_data['Registered For'],
                registered_by=form_data['Registered By'],
                last_updated=datetime.now(),
                is_complete=form_data.get('Complete', False)
            )
            session.add(new_record)
            session.flush()

            # ثبت مصرف‌ها و آپدیت همزمان mto_progress
            for item in consumption_items:
                # ثبت مصرف
                consumption = MTOConsumption(
                    mto_item_id=item['mto_item_id'],
                    miv_record_id=new_record.id,
                    used_qty=item['used_qty']
                )
                session.add(consumption)

                # گرفتن آیتم MTO
                mto_item = session.query(MTOItem).filter(MTOItem.id == item['mto_item_id']).first()
                if mto_item:
                    total_qty = (mto_item.length_m if mto_item.item_type and 'pipe' in mto_item.item_type.lower()
                                 else mto_item.quantity) or 0
                    used_qty = (
                                   session.query(func.coalesce(func.sum(MTOConsumption.used_qty), 0.0))
                                   .filter(MTOConsumption.mto_item_id == mto_item.id)
                                   .scalar()
                               ) or 0
                    remaining_qty = max(0, total_qty - used_qty)

                    # پیدا کردن یا ساخت رکورد mto_progress
                    progress_row = session.query(MTOProgress).filter(
                        MTOProgress.project_id == project_id,
                        MTOProgress.line_no == form_data['Line No'],
                        MTOProgress.mto_item_id == mto_item.id
                    ).first()

                    if progress_row:
                        progress_row.total_qty = total_qty
                        progress_row.used_qty = used_qty
                        progress_row.remaining_qty = remaining_qty
                        progress_row.last_updated = datetime.now()
                    else:
                        new_progress = MTOProgress(
                            project_id=project_id,
                            line_no=form_data['Line No'],
                            mto_item_id=mto_item.id,
                            item_code=mto_item.item_code,
                            description=mto_item.description,
                            unit=mto_item.unit,
                            total_qty=total_qty,
                            used_qty=used_qty,
                            remaining_qty=remaining_qty,
                            last_updated=datetime.now()
                        )
                        session.add(new_progress)

            session.commit()

            # ثبت لاگ
            self.log_activity(
                user=form_data['Registered By'],
                action="REGISTER_MIV",
                details=f"MIV Tag '{form_data['MIV Tag']}' for Line '{form_data['Line No']}' in project ID {project_id}"
            )
            return True, "رکورد با موفقیت ثبت شد."

        except Exception as e:
            session.rollback()
            return False, f"خطا در ثبت رکورد: {e}"
        finally:
            session.close()


    def update_miv_record(self, record_id, updated_data, new_consumption_items=None, user="system"):
        """
        یک رکورد MIV را آپدیت می‌کند (هم داده‌های اصلی و هم مصرف‌ها).
        پس از هر آپدیت، جدول MTOProgress را برای خط مربوطه بازسازی می‌کند.
        """
        session = self.get_session()
        try:
            record = session.query(MIVRecord).filter(MIVRecord.id == record_id).first()
            if not record:
                return False, f"رکورد با شناسه {record_id} یافت نشد."

            # ۱. آپدیت فیلدهای اصلی رکورد (مانند location, status, comment)
            for key, value in updated_data.items():
                if hasattr(record, key):
                    setattr(record, key, value)
            record.last_updated = datetime.now()

            # ۲. اگر لیست مصرف جدیدی ارائه شده باشد، جایگزین قبلی‌ها کن
            if new_consumption_items is not None:
                # پاک کردن تمام مصرف‌های قبلی این MIV
                session.query(MTOConsumption).filter(MTOConsumption.miv_record_id == record_id).delete()

                # اضافه کردن مصرف‌های جدید
                for item in new_consumption_items:
                    consumption = MTOConsumption(
                        mto_item_id=item['mto_item_id'],
                        miv_record_id=record.id,
                        used_qty=item['used_qty']
                    )
                    session.add(consumption)

            session.commit()

            # ۳. (مهم) بروزرسانی جدول MTOProgress برای خط مربوطه با بازسازی کامل
            self.rebuild_mto_progress_for_line(record.project_id, record.line_no)

            # ثبت لاگ
            self.log_activity(
                user=user,
                action="UPDATE_MIV",
                details=f"Record ID {record_id} updated in project {record.project_id}, line {record.line_no}"
            )

            return True, "رکورد با موفقیت ویرایش شد."

        except Exception as e:
            session.rollback()
            logging.error(f"خطا در به‌روزرسانی رکورد {record_id}: {e}")
            return False, f"خطا در به‌روزرسانی رکورد: {e}"
        finally:
            session.close()

    def delete_miv_record(self, record_id):
        """
        یک رکورد MIV و تمام مصرف‌های مرتبط با آن را حذف می‌کند.
        سپس جدول MTOProgress را برای آن خط به‌طور کامل بازسازی می‌کند تا آمار دقیق شود.
        """
        session = self.get_session()
        try:
            # ۱. رکورد اصلی را پیدا کن تا به اطلاعات پروژه و خط دسترسی داشته باشی
            record = session.query(MIVRecord).filter(MIVRecord.id == record_id).first()
            if not record:
                return False, "رکورد یافت نشد."

            project_id = record.project_id
            line_no = record.line_no
            miv_tag = record.miv_tag

            # ۲. تمام رکوردهای مصرفی مرتبط با این MIV را حذف کن
            session.query(MTOConsumption).filter(MTOConsumption.miv_record_id == record_id).delete()

            # ۳. خود رکورد MIV را حذف کن
            session.delete(record)
            session.commit()

            # ۴. (مهم) آمار پیشرفت را برای این خط از نو بساز
            self.rebuild_mto_progress_for_line(project_id, line_no)

            # ۵. ثبت لاگ
            self.log_activity(
                user="system",  # در عمل نام کاربر لاگین شده قرار می‌گیرد
                action="DELETE_MIV",
                details=f"Deleted MIV Record ID {record_id} (Tag: {miv_tag}) from project {project_id}, line {line_no}"
            )

            return True, "رکورد و مصرف‌های مرتبط با موفقیت حذف شدند."

        except Exception as e:
            session.rollback()
            logging.error(f"خطا در حذف رکورد MIV با شناسه {record_id}: {e}")
            return False, f"خطا در حذف رکورد: {e}"
        finally:
            session.close()

    def rebuild_mto_progress_for_line(self, project_id, line_no):
        """
        جدول MTOProgress برای یک خط را از صفر بازسازی می‌کند.
        """
        session = self.get_session()
        try:
            # همه رکوردهای قدیمی progress برای این خط رو حذف کن
            session.query(MTOProgress).filter(
                MTOProgress.project_id == project_id,
                MTOProgress.line_no == line_no
            ).delete()

            # دوباره از روی MTOItem + MTOConsumption بساز
            mto_items = session.query(MTOItem).filter(
                MTOItem.project_id == project_id,
                MTOItem.line_no == line_no
            ).all()

            for item in mto_items:
                if item.item_type and "pipe" in (item.item_type or "").lower():
                    total_required = item.length_m or 0
                else:
                    total_required = item.quantity or 0

                total_used = session.query(func.coalesce(func.sum(MTOConsumption.used_qty), 0.0)) \
                    .filter(MTOConsumption.mto_item_id == item.id).scalar()

                new_progress = MTOProgress(
                    project_id=project_id,
                    line_no=line_no,
                    mto_item_id=item.id,
                    item_code=item.item_code,
                    description=item.description,
                    unit=item.unit,
                    total_qty=total_required,
                    used_qty=total_used or 0,
                    remaining_qty=max(0, (total_required or 0) - (total_used or 0)),
                    last_updated=datetime.now()
                )
                session.add(new_progress)

            session.commit()
        except Exception as e:
            session.rollback()
            print(f"⚠️ خطا در بازسازی MTOProgress: {e}")
        finally:
            session.close()

    def update_miv_items(self, miv_record_id, updated_items):
        """
        آیتم‌های مصرفی یک MIV خاص را آپدیت می‌کند.
        updated_items: لیست جدید شامل {mto_item_id, used_qty}
        """
        session = self.get_session()
        try:
            record = session.query(MIVRecord).filter(MIVRecord.id == miv_record_id).first()
            if not record:
                return False, f"MIV با شناسه {miv_record_id} یافت نشد."

            # پاک کردن مصرف‌های قدیمی
            session.query(MTOConsumption).filter(MTOConsumption.miv_record_id == miv_record_id).delete()

            # ذخیره آیتم‌های جدید
            for item in updated_items:
                consumption = MTOConsumption(
                    mto_item_id=item["mto_item_id"],
                    miv_record_id=miv_record_id,
                    used_qty=item["used_qty"]
                )
                session.add(consumption)

            session.commit()

            # بازسازی Progress
            self.rebuild_mto_progress_for_line(record.project_id, record.line_no)

            self.log_activity(
                user="system",
                action="UPDATE_MIV_ITEMS",
                details=f"Items updated for MIV {miv_record_id}"
            )

            return True, "آیتم‌های مصرفی با موفقیت بروزرسانی شدند."
        except Exception as e:
            session.rollback()
            return False, f"خطا در بروزرسانی آیتم‌های MIV: {e}"
        finally:
            session.close()

    def update_miv_items(self, miv_record_id, updated_items, user="system"):
        """
        آیتم‌های مصرفی یک MIV خاص را آپدیت می‌کند و جدول پیشرفت را بازسازی می‌کند.
        updated_items: لیست جدید شامل {mto_item_id, used_qty}
        """
        session = self.get_session()
        try:
            record = session.query(MIVRecord).filter(MIVRecord.id == miv_record_id).first()
            if not record:
                return False, f"MIV با شناسه {miv_record_id} یافت نشد."

            # پاک کردن مصرف‌های قدیمی
            session.query(MTOConsumption).filter(MTOConsumption.miv_record_id == miv_record_id).delete()

            # ذخیره آیتم‌های جدید
            for item in updated_items:
                consumption = MTOConsumption(
                    mto_item_id=item["mto_item_id"],
                    miv_record_id=miv_record_id,
                    used_qty=item["used_qty"]
                )
                session.add(consumption)

            session.commit()

            # بازسازی Progress
            self.rebuild_mto_progress_for_line(record.project_id, record.line_no)

            self.log_activity(
                user=user,
                action="UPDATE_MIV_ITEMS",
                details=f"Consumption items updated for MIV {miv_record_id}"
            )

            return True, "آیتم‌های مصرفی با موفقیت بروزرسانی شدند."
        except Exception as e:
            session.rollback()
            logging.error(f"خطا در بروزرسانی آیتم‌های MIV {miv_record_id}: {e}")
            return False, f"خطا در بروزرسانی آیتم‌های MIV: {e}"
        finally:
            session.close()

    def get_consumptions_for_miv(self, miv_record_id):
        """
        تمام آیتم‌های مصرفی ثبت‌شده برای یک MIV خاص را برمی‌گرداند.
        خروجی یک دیکشنری است که کلید آن mto_item_id و مقدار آن used_qty است.
        """
        session = self.get_session()
        try:
            consumptions = session.query(MTOConsumption).filter(
                MTOConsumption.miv_record_id == miv_record_id
            ).all()
            # تبدیل لیست به دیکشنری برای دسترسی سریع‌تر
            return {item.mto_item_id: item.used_qty for item in consumptions}
        except Exception as e:
            logging.error(f"Error fetching consumptions for MIV {miv_record_id}: {e}")
            return {}
        finally:
            session.close()

    # --------------------------------------------------------------------
    # متدهای جستجو و اعتبارسنجی
    # --------------------------------------------------------------------

    def is_duplicate_miv_tag(self, miv_tag, project_id):
        """بررسی می‌کند که آیا یک MIV Tag در یک پروژه خاص تکراری است یا خیر."""
        session = self.get_session()
        try:
            exists = session.query(MIVRecord.id).filter(
                MIVRecord.project_id == project_id,
                MIVRecord.miv_tag == miv_tag
            ).first()
            return exists is not None
        finally:
            session.close()

    def get_line_no_suggestions(self, typed_text, top_n=7):
        """
        در تمام پروژه‌ها جستجو کرده و شماره خط‌های مشابه را به همراه نام پروژه پیشنهاد می‌دهد.
        این متد دیگر به project_id نیاز ندارد و به صورت سراسری عمل می‌کند.
        """
        if not typed_text or len(typed_text) < 2:
            return []

        session = self.get_session()
        try:
            # ۱. جستجوی سراسری با JOIN برای واکشی نام پروژه
            query = session.query(
                MTOItem.line_no,
                Project.name,
                Project.id
            ).join(Project, MTOItem.project_id == Project.id).distinct()

            all_lines_data = query.all()

            # ۲. نرمال‌سازی ورودی
            norm_input = str(typed_text).replace(" ", "").lower()

            # ۳. محاسبه شباهت
            matches = []
            seen_lines = set()  # برای جلوگیری از نمایش خطوط تکراری از یک پروژه

            for line_no, project_name, project_id in all_lines_data:
                if not line_no or (line_no, project_name) in seen_lines:
                    continue

                norm_line = str(line_no).replace(" ", "").lower()
                ratio = difflib.SequenceMatcher(None, norm_input, norm_line).ratio()

                if norm_input in norm_line:
                    ratio += 0.2

                if ratio > 0.4:
                    # ۴. ساخت یک دیکشنری کامل از اطلاعات برای هر پیشنهاد
                    matches.append({
                        'ratio': ratio,
                        'display': f"{line_no}  ({project_name})",  # متن نمایشی برای کاربر
                        'line_no': line_no,
                        'project_name': project_name,
                        'project_id': project_id
                    })
                    seen_lines.add((line_no, project_name))

            # ۵. مرتب‌سازی و برگرداندن N نتیجه برتر
            matches.sort(key=lambda x: x['ratio'], reverse=True)

            return matches[:top_n]

        except Exception as e:
            print(f"⚠️ خطا در پیشنهاد سراسری شماره خط: {e}")
            return []
        finally:
            session.close()

    def search_miv_by_line_no(self, project_id, line_no):
        """تمام رکوردهای MIV مربوط به یک شماره خط در یک پروژه را برمی‌گرداند."""
        session = self.get_session()
        try:
            records = session.query(MIVRecord).filter(
                MIVRecord.project_id == project_id,
                MIVRecord.line_no == line_no
            ).all()
            return records
        finally:
            session.close()

    # --------------------------------------------------------------------
    # متدهای دریافت داده برای نمایش (Viewers & Tables)
    # --------------------------------------------------------------------

    def get_miv_data(self, project_id, mode='all', line_no=None, last_n=None):
        """داده‌های MIV را برای نمایش در جدول بر اساس حالت‌های مختلف برمی‌گرداند."""
        session = self.get_session()
        try:
            query = session.query(MIVRecord).filter(MIVRecord.project_id == project_id)

            if mode == 'complete':
                query = query.filter(MIVRecord.is_complete == True)
            elif mode == 'incomplete':
                query = query.filter(MIVRecord.is_complete == False)

            if line_no:
                query = query.filter(MIVRecord.line_no == line_no)

            if last_n:
                query = query.order_by(MIVRecord.last_updated.desc()).limit(last_n)

            return query.all()
        finally:
            session.close()

    def get_mto_items_for_line(self, project_id, line_no):
        """تمام آیتم‌های MTO برای یک شماره خط خاص را برمی‌گرداند."""
        session = self.get_session()
        try:
            # این کوئری تمام اطلاعات لازم برای پنجره مصرف را برمی‌گرداند
            items = session.query(MTOItem).filter(
                MTOItem.project_id == project_id,
                MTOItem.line_no == line_no
            ).all()
            return items
        finally:
            session.close()

    def get_all_projects(self):
        """لیست همه پروژه‌ها را برمی‌گرداند."""
        session = self.get_session()
        try:
            return session.query(Project).order_by(Project.name).all()
        finally:
            session.close()

    def get_project_by_name(self, name):
        """پروژه را بر اساس نام دقیق آن جستجو می‌کند."""
        session = self.get_session()
        try:
            return session.query(Project).filter(Project.name == name).first()
        finally:
            session.close()

    # --------------------------------------------------------------------
    # متدهای گزارش‌گیری (مربوط به داشبورد و گزارش‌ها)
    # --------------------------------------------------------------------
    # متدهای get_project_progress, get_line_progress و generate_project_report که قبلاً نوشته‌اید
    # در اینجا قرار می‌گیرند و کامل هستند.
    @lru_cache(maxsize=128)
    def get_project_progress(self, project_id, default_diameter=1):
        """
        محاسبه پیشرفت کلی پروژه بر اساس داده‌های دیتابیس
        - وزن هر خط = (مجموع LENGTH(M) + QUANTITY) × بیشترین قطر پایپ در آن خط
        - درصد پیشرفت = وزن انجام‌شده / وزن کل × 100
        """
        from models import MTOItem, MTOConsumption, MIVRecord

        session = self.get_session()
        try:
            # گرفتن تمام شماره خطوط پروژه
            lines = session.query(MTOItem.line_no).filter(MTOItem.project_id == project_id).distinct().all()
            if not lines:
                return {"total_lines": 0, "total_weight": 0, "done_weight": 0, "percentage": 0}

            total_weight = 0
            done_weight = 0

            for (line_no,) in lines:
                # گرفتن تمام آیتم‌های این خط
                items = session.query(MTOItem).filter(
                    MTOItem.project_id == project_id,
                    MTOItem.line_no == line_no
                ).all()

                if not items:
                    continue

                # بیشترین قطر پایپ در این خط (در MTOItem پیکسلی نیست ولی میشه به item_type استناد کرد)
                max_diameter = default_diameter
                for item in items:
                    if item.item_type and "pipe" in item.item_type.lower():
                        try:
                            # فرض: طول یا قطر پایپ در description یا unit ذخیره نشده، فعلاً پیش‌فرض می‌زنیم
                            pass
                        except:
                            pass

                # مجموع مقادیر طول و تعداد
                length_sum = sum(item.length_m or 0 for item in items)
                qty_sum = sum(item.quantity or 0 for item in items)
                qty_sum_effective = length_sum + qty_sum

                line_weight = qty_sum_effective * max_diameter
                total_weight += line_weight

                # محاسبه مصرف‌شده
                used_qty = (
                               session.query(func.coalesce(func.sum(MTOConsumption.used_qty), 0))
                               .join(MTOItem, MTOConsumption.mto_item_id == MTOItem.id)
                               .filter(MTOItem.project_id == project_id, MTOItem.line_no == line_no)
                               .scalar()
                           ) or 0

                done_weight += used_qty * max_diameter

            percentage = round((done_weight / total_weight * 100), 2) if total_weight > 0 else 0

            return {
                "total_lines": len(lines),
                "total_weight": total_weight,
                "done_weight": done_weight,
                "percentage": percentage
            }

        except Exception as e:
            print(f"⚠️ خطا در محاسبه پیشرفت پروژه: {e}")
            return {"total_lines": 0, "total_weight": 0, "done_weight": 0, "percentage": 0}
        finally:
            session.close()

    @lru_cache(maxsize=256)
    def get_line_progress(self, project_id, line_no, readonly=True):  # 🔹 نیازی به default_diameter نیست
        """
        محاسبه پیشرفت یک خط خاص در پروژه با استفاده از داده‌های MTOProgress.
        """
        session = self.get_session()
        try:
            # جمع کل و مصرف شده از رکوردهای MTOProgress
            # ما فرض می‌کنیم هر آیتم وزن یکسانی دارد. اگر وزن‌دهی پیچیده‌تر نیاز بود، منطق تغییر می‌کند.
            query_result = session.query(
                func.sum(MTOProgress.total_qty),
                func.sum(MTOProgress.used_qty)
            ).filter(
                MTOProgress.project_id == project_id,
                MTOProgress.line_no == line_no
            ).first()

            total_weight, done_weight = query_result
            total_weight = total_weight or 0
            done_weight = done_weight or 0

            if total_weight == 0 and not readonly:
                # اگر خط داده‌ای در MTOProgress نداشت، یک بار آن را بساز
                self.initialize_mto_progress_for_line(project_id, line_no)
                query_result = session.query(
                    func.sum(MTOProgress.total_qty),
                    func.sum(MTOProgress.used_qty)
                ).filter(
                    MTOProgress.project_id == project_id,
                    MTOProgress.line_no == line_no
                ).first()
                total_weight, done_weight = query_result
                total_weight = total_weight or 0
                done_weight = done_weight or 0

            percentage = round((done_weight / total_weight * 100), 2) if total_weight > 0 else 0

            return {
                "line_no": line_no,
                "total_weight": total_weight,
                "done_weight": done_weight,
                "percentage": percentage
            }

        except Exception as e:
            print(f"⚠️ خطا در محاسبه پیشرفت خط {line_no}: {e}")
            return {"line_no": line_no, "total_weight": 0, "done_weight": 0, "percentage": 0}
        finally:
            session.close()

    def generate_project_report(self, project_id):
        """
        تولید گزارش کامل پیشرفت پروژه
        شامل درصد پیشرفت کلی و جزئیات هر خط
        """
        report = {
            "project_id": project_id,
            "summary": self.get_project_progress(project_id),
            "lines": []
        }

        # گرفتن شماره تمام خطوط پروژه
        from models import MTOItem
        session = self.get_session()
        try:
            lines = (
                session.query(MTOItem.line_no)
                .filter(MTOItem.project_id == project_id)
                .distinct()
                .all()
            )
            for (line_no,) in lines:
                line_progress = self.get_line_progress(project_id, line_no)
                report["lines"].append(line_progress)

            # ثبت گزارش به عنوان فعالیت
            self.log_activity(
                user="system",  # یا نام کاربری که لاگین کرده
                action="GENERATE_REPORT",
                details=f"Generated progress report for project ID {project_id}"
            )

            return report

        except Exception as e:
            print(f"⚠️ خطا در تولید گزارش پروژه {project_id}: {e}")
            return report
        finally:
            session.close()

        # متدهای مدیریت پروژه و اعتبارسنجی (باقی‌مانده از MIVRegistry)
        # --------------------------------------------------------------------

    def rename_project(self, project_id, new_name, user="system"):
        """نام یک پروژه را تغییر می‌دهد."""
        session = self.get_session()
        try:
            project = session.query(Project).filter(Project.id == project_id).first()
            if project:
                original_name = project.name
                # بررسی اینکه نام جدید تکراری نباشد
                name_exists = session.query(Project.id).filter(Project.name == new_name).first()
                if name_exists:
                    return False, f"پروژه‌ای با نام '{new_name}' از قبل وجود دارد."

                project.name = new_name
                session.commit()
                self.log_activity(user, "RENAME_PROJECT", f"Project '{original_name}' renamed to '{new_name}'")
                return True, f"نام پروژه با موفقیت به '{new_name}' تغییر یافت."
            return False, "پروژه یافت نشد."
        except Exception as e:
            session.rollback()
            return False, f"خطا در تغییر نام پروژه: {e}"
        finally:
            session.close()

    def copy_line_to_project(self, line_no, from_project_id, to_project_id, user="system"):
        """تمام رکوردهای MIV یک خط را از پروژه‌ای به پروژه دیگر کپی می‌کند."""
        session = self.get_session()
        try:
            records_to_copy = session.query(MIVRecord).filter(
                MIVRecord.project_id == from_project_id,
                MIVRecord.line_no == line_no
            ).all()

            if not records_to_copy:
                return False, "هیچ رکوردی برای کپی یافت نشد."

            for record in records_to_copy:
                # برای جلوگیری از تکراری شدن تگ، یک پسوند به آن اضافه می‌کنیم
                new_tag = f"{record.miv_tag}-COPY-{datetime.now().strftime('%f')}"

                new_record = MIVRecord(
                    project_id=to_project_id,
                    line_no=record.line_no,
                    miv_tag=new_tag,
                    location=record.location,
                    status=record.status,
                    comment=f"Copied from project ID {from_project_id}",
                    registered_for=record.registered_for,
                    registered_by=user,
                    is_complete=record.is_complete,
                    last_updated=datetime.now()
                )
                session.add(new_record)

            session.commit()
            self.log_activity(user, "COPY_LINE",
                              f"Line '{line_no}' copied from project {from_project_id} to {to_project_id}")
            return True, "خط با موفقیت کپی شد."
        except Exception as e:
            session.rollback()
            return False, f"خطا در کپی کردن خط: {e}"
        finally:
            session.close()

    def check_duplicates_in_project(self, project_id, column_name='miv_tag'):
        """رکوردهای با مقدار تکراری در یک ستون خاص (مثل miv_tag) را پیدا می‌کند."""
        session = self.get_session()
        try:
            if not hasattr(MIVRecord, column_name):
                return None, f"ستونی با نام '{column_name}' در مدل MIVRecord وجود ندارد."

            column = getattr(MIVRecord, column_name)

            # پیدا کردن مقادیر تکراری
            duplicates_query = session.query(column, func.count(MIVRecord.id).label('count')). \
                filter(MIVRecord.project_id == project_id). \
                group_by(column). \
                having(func.count(MIVRecord.id) > 1).subquery()

            # گرفتن تمام رکوردهایی که مقدار ستونشان تکراری است
            final_query = session.query(MIVRecord).join(
                duplicates_query, column == duplicates_query.c[column_name]
            ).order_by(column)

            return final_query.all(), None
        except Exception as e:
            return None, f"خطا در بررسی موارد تکراری: {e}"
        finally:
            session.close()

    def is_line_complete(self, project_id, line_no):
        session = self.get_session()
        try:
            mto_items = session.query(MTOItem).filter(
                MTOItem.project_id == project_id,
                MTOItem.line_no == line_no
            ).all()

            if not mto_items:
                return False

            for item in mto_items:
                if item.item_type and 'pipe' in item.item_type.lower():
                    total_required = item.length_m or 0
                else:
                    total_required = item.quantity or 0

                total_used = session.query(func.coalesce(func.sum(MTOConsumption.used_qty), 0.0)) \
                    .filter(MTOConsumption.mto_item_id == item.id).scalar()

                if total_used < total_required:
                    return False
            return True
        finally:
            session.close()

    def get_line_material_progress(self, project_id, line_no, readonly=True):
        """
        داده‌های پیشرفت متریال یک خط را مستقیماً از جدول MTOProgress می‌خواند.
        این متد بهینه شده و منبع اصلی داده برای پنجره مصرف است.
        """
        session = self.get_session()
        try:
            # به جای محاسبه مجدد، مستقیماً از جدول پیشرفت می‌خوانیم
            progress_items = session.query(MTOProgress).filter(
                MTOProgress.project_id == project_id,
                MTOProgress.line_no == line_no
            ).all()

            # اگر هیچ رکوردی در جدول پیشرفت نبود، آن را از MTOItem بساز
            if not progress_items and not readonly:
                self.initialize_mto_progress_for_line(project_id, line_no)
                progress_items = session.query(MTOProgress).filter(
                    MTOProgress.project_id == project_id,
                    MTOProgress.line_no == line_no
                ).all()

            progress_data = []
            for item in progress_items:
                progress_data.append({
                    "mto_item_id": item.mto_item_id,  # 🔹 مهم: ID را برای استفاده در UI اضافه می‌کنیم
                    "Item Code": item.item_code,
                    "Description": item.description,
                    "Unit": item.unit,
                    "Total Qty": item.total_qty or 0,
                    "Used Qty": item.used_qty or 0,
                    "Remaining Qty": item.remaining_qty or 0
                })
            return progress_data
        finally:
            session.close()

    def initialize_mto_progress_for_line(self, project_id, line_no):
        """
        برای یک خط که هنوز رکوردی در MTOProgress ندارد، رکوردهای اولیه را ایجاد می‌کند.
        این تابع زمانی فراخوانی می‌شود که برای اولین بار اطلاعات یک خط درخواست می‌شود.
        """
        session = self.get_session()
        try:
            mto_items = session.query(MTOItem).filter(
                MTOItem.project_id == project_id,
                MTOItem.line_no == line_no
            ).all()

            for item in mto_items:
                # چک می‌کند که آیا رکوردی برای این آیتم وجود دارد یا نه
                exists = session.query(MTOProgress).filter(
                    MTOProgress.mto_item_id == item.id
                ).first()

                if not exists:
                    if item.item_type and 'pipe' in item.item_type.lower():
                        total_required = item.length_m or 0
                    else:
                        total_required = item.quantity or 0

                    # مصرف قبلی را محاسبه کن (فقط برای بار اول)
                    total_used = session.query(func.coalesce(func.sum(MTOConsumption.used_qty), 0.0)) \
                        .filter(MTOConsumption.mto_item_id == item.id).scalar()

                    new_progress = MTOProgress(
                        project_id=project_id,
                        line_no=line_no,
                        mto_item_id=item.id,
                        item_code=item.item_code,
                        description=item.description,
                        unit=item.unit,
                        total_qty=total_required,
                        used_qty=total_used,
                        remaining_qty=max(0, total_required - total_used),
                        last_updated=datetime.now()
                    )
                    session.add(new_progress)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"⚠️ خطا در ساخت پیش‌فرض MTO Progress: {e}")
        finally:
            session.close()

    def get_data_as_dataframe(self, model_class, project_id=None):
        """
        داده‌های یک جدول (مدل) را به صورت پانداز DataFrame برمی‌گرداند.
        این متد برای خروجی گرفتن اکسل بسیار مفید است.
        """
        session = self.get_session()
        try:
            query = session.query(model_class)
            if project_id and hasattr(model_class, 'project_id'):
                query = query.filter(model_class.project_id == project_id)

            # استفاده از pd.read_sql برای کارایی بهتر
            df = pd.read_sql(query.statement, session.bind)
            return df
        except Exception as e:
            print(f"Error converting table to DataFrame: {e}")
            return pd.DataFrame()
        finally:
            session.close()

    def backup_database(self, backup_dir="."):
        """از کل فایل پایگاه داده یک نسخه پشتیبان تهیه می‌کند."""
        import shutil

        db_file = self.engine.url.database
        if not os.path.exists(db_file):
            return False, "فایل پایگاه داده یافت نشد."

        try:
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)

            backup_name = f"backup_{os.path.basename(db_file)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            backup_path = os.path.join(backup_dir, backup_name)

            shutil.copy2(db_file, backup_path)
            return True, f"پشتیبان‌گیری با موفقیت در مسیر {backup_path} انجام شد."
        except Exception as e:
            return False, f"خطا در پشتیبان‌گیری: {e}"

    def update_mto_progress(self, project_id, line_no, updates):
        """
        بروزرسانی جدول mto_progress بر اساس آیتم‌های مصرفی جدید.
        updates: لیستی از تاپل‌ها (item_code, qty, unit, description)
        """
        session = self.get_session()
        try:
            for item_code, qty, unit, desc in updates:
                # پیدا کردن آیتم‌ها از MTOItem
                query = session.query(MTOItem).filter(
                    MTOItem.project_id == project_id,
                    MTOItem.line_no == line_no
                )

                if item_code and str(item_code).strip():
                    query = query.filter(MTOItem.item_code == str(item_code).strip())
                else:
                    query = query.filter(MTOItem.description == str(desc).strip())

                mto_items = query.all()

                # محاسبه Total Qty
                total_qty = 0
                for mto_item in mto_items:
                    if mto_item.item_type and "pipe" in mto_item.item_type.lower():
                        total_qty += mto_item.length_m or 0
                    else:
                        total_qty += mto_item.quantity or 0

                # محاسبه Used Qty
                used_qty = 0
                for mto_item in mto_items:
                    used_qty += (
                                    session.query(func.coalesce(func.sum(MTOConsumption.used_qty), 0.0))
                                    .filter(MTOConsumption.mto_item_id == mto_item.id)
                                    .scalar()
                                ) or 0

                remaining_qty = max(0, total_qty - used_qty)

                # پیدا کردن یا ساخت رکورد در MTOProgress
                progress_row = session.query(MTOProgress).filter(
                    MTOProgress.line_no == line_no,
                    MTOProgress.item_code == (item_code or "")
                ).first()

                if progress_row:
                    progress_row.total_qty = total_qty
                    progress_row.used_qty = used_qty
                    progress_row.remaining_qty = remaining_qty
                    progress_row.last_updated = datetime.now()
                else:
                    new_progress = MTOProgress(
                        line_no=line_no,
                        item_code=item_code or "",
                        description=desc,
                        unit=unit,
                        total_qty=total_qty,
                        used_qty=used_qty,
                        remaining_qty=remaining_qty,
                        last_updated=datetime.now()
                    )
                    session.add(new_progress)

            session.commit()
        except Exception as e:
            session.rollback()
            print(f"⚠️ خطا در بروزرسانی mto_progress: {e}")
        finally:
            session.close()

    def get_used_qty(self, project_id, line_no, item_code=None, description=None):
        session = self.get_session()
        try:
            query = session.query(func.coalesce(func.sum(MTOConsumption.used_qty), 0.0)) \
                .join(MTOItem, MTOConsumption.mto_item_id == MTOItem.id) \
                .filter(MTOItem.project_id == project_id, MTOItem.line_no == line_no)

            if item_code and str(item_code).strip():
                query = query.filter(MTOItem.item_code == str(item_code).strip())
            elif description:
                query = query.filter(MTOItem.description == str(description).strip())

            return query.scalar() or 0
        finally:
            session.close()

    def suggest_line_no(self, project_id, line_no_input):
        session = self.get_session()
        try:
            all_lines = [x[0] for x in session.query(MTOItem.line_no)
            .filter(MTOItem.project_id == project_id)
            .distinct().all()]
            norm_input = str(line_no_input).replace(" ", "").lower()
            normalized_lines = {line: str(line).replace(" ", "").lower() for line in all_lines}

            best_match = None
            best_ratio = 0
            for original, normalized in normalized_lines.items():
                ratio = difflib.SequenceMatcher(None, norm_input, normalized).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_match = original

            return best_match if best_ratio > 0.6 else None
        finally:
            session.close()

    # --------------------------------------------------------------------
    # متدهایی که در API جدید صدا زدیم
    # --------------------------------------------------------------------

    def get_lines_for_project(self, project_id):
        """تمام شماره خط‌های متمایز برای یک پروژه را برمی‌گرداند."""
        session = self.get_session()
        try:
            # از جدول MTOItem شماره خطوط را می‌خوانیم
            lines = session.query(MTOItem.line_no).filter(MTOItem.project_id == project_id).distinct().order_by(MTOItem.line_no).all()
            # نتیجه کوئری لیستی از tupleهاست، آن را به لیست رشته تبدیل می‌کنیم
            return [line[0] for line in lines]
        except Exception as e:
            logging.error(f"Error fetching lines for project {project_id}: {e}")
            return []
        finally:
            session.close()

    def get_activity_logs(self, limit=100):
        """آخرین N رکورد از جدول لاگ فعالیت‌ها را برمی‌گرداند."""
        session = self.get_session()
        try:
            return session.query(ActivityLog).order_by(ActivityLog.timestamp.desc()).limit(limit).all()
        except Exception as e:
            logging.error(f"Error fetching activity logs: {e}")
            return []
        finally:
            session.close()

    def get_project_analytics(self, project_id):
        """داده‌های تحلیلی و آماری یک پروژه را برای داشبورد استخراج می‌کند."""
        session = self.get_session()
        try:
            # 1. تحلیل فعالیت کاربران (تعداد MIV ثبت شده توسط هر کاربر)
            user_activity = session.query(
                MIVRecord.registered_by,
                func.count(MIVRecord.id).label('miv_count')
            ).filter(MIVRecord.project_id == project_id).group_by(MIVRecord.registered_by).order_by(
                func.count(MIVRecord.id).desc()).all()

            # 2. تحلیل مصرف متریال (پر مصرف‌ترین آیتم‌ها)
            material_consumption = session.query(
                MTOItem.description,
                func.sum(MTOConsumption.used_qty).label('total_used')
            ).join(MTOConsumption, MTOItem.id == MTOConsumption.mto_item_id) \
                .filter(MTOItem.project_id == project_id) \
                .group_by(MTOItem.description).order_by(func.sum(MTOConsumption.used_qty).desc()).limit(10).all()

            # 3. تحلیل وضعیت MIV ها
            status_distribution = session.query(
                MIVRecord.status,
                func.count(MIVRecord.id).label('status_count')
            ).filter(MIVRecord.project_id == project_id, MIVRecord.status != None) \
                .group_by(MIVRecord.status).all()

            return {
                "user_activity": [{"user": user, "count": count} for user, count in user_activity],
                "material_consumption": [{"material": desc, "total_used": used} for desc, used in material_consumption],
                "status_distribution": [{"status": status, "count": count} for status, count in status_distribution]
            }
        except Exception as e:
            logging.error(f"Error fetching project analytics for project {project_id}: {e}")
            return {}
        finally:
            session.close()
