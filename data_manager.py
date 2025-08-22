# file: data_manager.py

import os
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, joinedload
from functools import lru_cache
from datetime import datetime
from models import Base, Project, MIVRecord, MTOItem, MTOConsumption, ActivityLog, MTOProgress, Spool, SpoolItem, \
    SpoolConsumption, SpoolProgress
import numpy as np
import pandas as pd
import difflib
# data_manager.py (در ابتدای فایل)
import logging
import re
from typing import Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SPOOL_TYPE_MAPPING = {
    "FLANGE": ("FLG", "FLAN", "FLN"),
    "ELBOW": ("ELB", "ELL", "ELBO"),
    "TEE": ("TEE",),
    "REDUCER": ("RED","REDU","CON","CONN", "ECC"),
    "CAP": ("CAP",),
    "PIPE": ("PIPE", "PIP"),

    # ... شما می‌توانید آیتم‌های بیشتری به اینجا اضافه کنید
}

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

    def log_activity(self, user, action, details="", session=None):
        """ثبت لاگ در جدول ActivityLog"""
        own_session = False
        if session is None:
            session = self.get_session()
            own_session = True

        try:
            log = ActivityLog(
                user=user,
                action=action,
                details=details,
                timestamp=datetime.now()
            )
            session.add(log)
            if own_session:
                session.commit()
        except Exception as e:
            if own_session:
                session.rollback()
            print(f"⚠️ خطا در ثبت لاگ: {e}")
        finally:
            if own_session:
                session.close()

    # --------------------------------------------------------------------
    # متدهای اصلی برای مدیریت رکوردها (CRUD Operations)
    # --------------------------------------------------------------------

    def register_miv_record(self, project_id, form_data, consumption_items, spool_consumption_items=None):
        session = self.get_session()
        try:
            # ... (بخش ساخت MIVRecord) ...
            new_record = MIVRecord(
                project_id=project_id,
                line_no=form_data['Line No'],
                miv_tag=form_data['MIV Tag'],
                location=form_data['Location'],
                status=form_data['Status'],
                comment=form_data.get('Comment', ''),
                registered_for=form_data['Registered For'],
                registered_by=form_data['Registered By'],
                last_updated=datetime.now(),
                is_complete=form_data.get('Complete', False)
            )
            session.add(new_record)
            session.flush()

            for item in consumption_items:
                session.add(MTOConsumption(
                    mto_item_id=item['mto_item_id'],
                    miv_record_id=new_record.id,
                    used_qty=item['used_qty'],  # از UI گرد شده می‌آید
                    timestamp=datetime.now()
                ))

            if spool_consumption_items:
                spool_notes = []
                for consumption in spool_consumption_items:
                    # ... (بخش کم کردن از موجودی اسپول بدون تغییر) ...
                    spool_item = session.get(SpoolItem, consumption['spool_item_id'])
                    used_qty = consumption['used_qty']
                    if not spool_item: raise ValueError(f"Spool item ID {consumption['spool_item_id']} not found.")
                    is_pipe = "PIPE" in (spool_item.component_type or "").upper()
                    if is_pipe:
                        if (spool_item.length or 0) < used_qty: raise ValueError(
                            f"Insufficient length for pipe in spool {spool_item.spool.spool_id}.")
                        spool_item.length -= used_qty
                    else:
                        if (spool_item.qty_available or 0) < used_qty: raise ValueError(
                            f"Insufficient qty for {spool_item.component_type} in spool {spool_item.spool.spool_id}.")
                        spool_item.qty_available -= used_qty

                    session.add(SpoolConsumption(
                        spool_item_id=spool_item.id,
                        spool_id=spool_item.spool.id,
                        miv_record_id=new_record.id,
                        used_qty=used_qty,  # از UI گرد شده می‌آید
                        timestamp=datetime.now()
                    ))

                    # --- CHANGE: اصلاح واحد در Note ---
                    unit = "m" if is_pipe else "عدد"
                    spool_notes.append(
                        f"{used_qty:.2f} {unit} از {spool_item.component_type} (اسپول: {spool_item.spool.spool_id})")

                if spool_notes:
                    final_comment = (new_record.comment or "") + " | مصرف اسپول: " + ", ".join(spool_notes)
                    new_record.comment = final_comment

            session.commit()
            self.rebuild_mto_progress_for_line(project_id, form_data['Line No'])

            self.log_activity(
                user=form_data['Registered By'], action="REGISTER_MIV",
                details=f"MIV Tag '{form_data['MIV Tag']}' for Line '{form_data['Line No']}'",
            )
            return True, "رکورد با موفقیت ثبت شد."

        except Exception as e:
            session.rollback()
            import traceback
            logging.error(f"خطا در ثبت رکورد: {e}\n{traceback.format_exc()}")
            return False, f"خطا در ثبت رکورد: {e}"
        finally:
            session.close()

    def update_miv_items(self, miv_record_id, updated_items, updated_spool_items, user="system"):
        session = self.get_session()
        try:
            record = session.get(MIVRecord, miv_record_id)
            if not record:
                return False, f"MIV با شناسه {miv_record_id} یافت نشد."

            project_id = record.project_id
            line_no = record.line_no

            # --- مدیریت مصرف اسپول ---
            # 1. بازگرداندن موجودی‌های قدیمی اسپول به انبار
            old_spool_consumptions = session.query(SpoolConsumption).filter(
                SpoolConsumption.miv_record_id == miv_record_id).all()
            for old_c in old_spool_consumptions:
                spool_item = session.get(SpoolItem, old_c.spool_item_id)
                if spool_item:
                    is_pipe = "PIPE" in (spool_item.component_type or "").upper()
                    if is_pipe:
                        spool_item.length = (spool_item.length or 0) + old_c.used_qty
                    else:
                        spool_item.qty_available = (spool_item.qty_available or 0) + old_c.used_qty

            # 2. حذف رکوردهای مصرف قدیمی (هم MTO و هم Spool)
            session.query(MTOConsumption).filter(MTOConsumption.miv_record_id == miv_record_id).delete()
            session.query(SpoolConsumption).filter(SpoolConsumption.miv_record_id == miv_record_id).delete()
            session.flush()

            # --- ثبت مصرف‌های جدید ---
            # 3. ثبت مصرف مستقیم MTO
            for item in updated_items:
                session.add(MTOConsumption(
                    mto_item_id=item["mto_item_id"],
                    miv_record_id=miv_record_id,
                    used_qty=item["used_qty"],
                    timestamp=datetime.now()
                ))

            # 4. ثبت مصرف جدید اسپول
            spool_notes = []
            if updated_spool_items:
                for s_item in updated_spool_items:
                    spool_item = session.get(SpoolItem, s_item['spool_item_id'])
                    used_qty = s_item['used_qty']

                    if not spool_item:
                        raise ValueError(f"آیتم اسپول با شناسه {s_item['spool_item_id']} یافت نشد.")

                    is_pipe = "PIPE" in (spool_item.component_type or "").upper()

                    if is_pipe:
                        if (spool_item.length or 0) < used_qty:
                            raise ValueError(f"طول موجود پایپ در اسپول {spool_item.spool.spool_id} کافی نیست.")
                        spool_item.length -= used_qty
                    else:
                        if (spool_item.qty_available or 0) < used_qty:
                            raise ValueError(
                                f"موجودی آیتم {spool_item.component_type} در اسپول {spool_item.spool.spool_id} کافی نیست.")
                        spool_item.qty_available -= used_qty

                    session.add(SpoolConsumption(
                        spool_item_id=spool_item.id,
                        spool_id=spool_item.spool.id,
                        miv_record_id=miv_record_id,
                        used_qty=used_qty,
                        timestamp=datetime.now()
                    ))
                    # ساخت Note
                    unit = "mm" if is_pipe else "عدد"
                    spool_notes.append(
                        f"{used_qty:.1f} {unit} از {spool_item.component_type} (اسپول: {spool_item.spool.spool_id})")

            # 5. (مهم) بازسازی کامل آمار خط بعد از تمام تغییرات
            session.commit()
            self.rebuild_mto_progress_for_line(project_id, line_no)

            self.log_activity(
                user=user,
                action="UPDATE_MIV_ITEMS",
                details=f"Consumption items updated for MIV {miv_record_id}",
            )
            return True, "آیتم‌های مصرفی با موفقیت بروزرسانی شدند."

        except Exception as e:
            session.rollback()
            import traceback
            logging.error(f"خطا در بروزرسانی آیتم‌های MIV {miv_record_id}: {e}\n{traceback.format_exc()}")
            return False, f"خطا در بروزرسانی آیتم‌های MIV: {e}"
        finally:
            session.close()

    def delete_miv_record(self, record_id, user="system"):
        """
        یک رکورد MIV و تمام مصرف‌های مرتبط با آن (MTO و Spool) را حذف می‌کند.
        موجودی مصرف شده از اسپول‌ها را به انبار برمی‌گرداند.
        سپس جدول MTOProgress را برای آن خط به‌طور کامل بازسازی می‌کند.
        """
        session = self.get_session()
        try:
            # ۱. رکورد اصلی را پیدا کن
            record = session.get(MIVRecord, record_id)
            if not record:
                return False, "رکورد یافت نشد."

            project_id = record.project_id
            line_no = record.line_no
            miv_tag = record.miv_tag

            # ۲. (مهم) موجودی‌های مصرفی اسپول را به انبار برگردان
            spool_consumptions = session.query(SpoolConsumption).filter(SpoolConsumption.miv_record_id == record_id).all()
            for consumption in spool_consumptions:
                spool_item = session.get(SpoolItem, consumption.spool_item_id)
                if spool_item:
                    is_pipe = "PIPE" in (spool_item.component_type or "").upper()
                    if is_pipe:
                        spool_item.length = (spool_item.length or 0) + consumption.used_qty
                    else:
                        spool_item.qty_available = (spool_item.qty_available or 0) + consumption.used_qty

            # ۳. تمام رکوردهای مصرفی مرتبط (MTO و Spool) را حذف کن
            session.query(MTOConsumption).filter(MTOConsumption.miv_record_id == record_id).delete()
            session.query(SpoolConsumption).filter(SpoolConsumption.miv_record_id == record_id).delete()

            # ۴. خود رکورد MIV را حذف کن
            session.delete(record)
            session.commit()

            # ۵. (مهم) آمار پیشرفت را برای این خط از نو بساز
            self.rebuild_mto_progress_for_line(project_id, line_no)

            # ۶. ثبت لاگ
            self.log_activity(
                user=user,
                action="DELETE_MIV",
                details=f"Deleted MIV Record ID {record_id} (Tag: {miv_tag}) for line {line_no}"
            )

            return True, "رکورد و مصرف‌های مرتبط با موفقیت حذف شدند."

        except Exception as e:
            session.rollback()
            logging.error(f"خطا در حذف رکورد MIV با شناسه {record_id}: {e}")
            return False, f"خطا در حذف رکورد: {e}"
        finally:
            session.close()

    def rebuild_mto_progress_for_line(self, project_id, line_no):
        session = self.get_session()
        try:
            mto_items_in_line = session.query(MTOItem).filter(
                MTOItem.project_id == project_id,
                MTOItem.line_no == line_no
            ).all()

            for mto_item in mto_items_in_line:
                # --- CHANGE: حذف تبدیل واحد ---
                is_pipe = mto_item.item_type and 'pipe' in mto_item.item_type.lower()
                total_required = mto_item.length_m or 0 if is_pipe else mto_item.quantity or 0

                direct_used = session.query(func.coalesce(func.sum(MTOConsumption.used_qty), 0.0)).filter(
                    MTOConsumption.mto_item_id == mto_item.id
                ).scalar()

                # ... (منطق پیدا کردن spool_equivalents بدون تغییر) ...
                mto_type_upper = str(mto_item.item_type).upper().strip()
                spool_equivalents = [mto_type_upper]
                for key, aliases in SPOOL_TYPE_MAPPING.items():
                    if mto_type_upper == key or mto_type_upper in aliases:
                        spool_equivalents.extend([key] + list(aliases));
                        break
                spool_equivalents = list(set(spool_equivalents))

                spool_used_query = session.query(func.coalesce(func.sum(SpoolConsumption.used_qty), 0.0)).join(
                    MIVRecord, SpoolConsumption.miv_record_id == MIVRecord.id
                ).join(
                    SpoolItem, SpoolConsumption.spool_item_id == SpoolItem.id
                ).filter(
                    MIVRecord.line_no == line_no,
                    MIVRecord.project_id == project_id,
                    func.upper(SpoolItem.component_type).in_(spool_equivalents),
                )
                if mto_item.p1_bore_in is not None:
                    spool_used_query = spool_used_query.filter(SpoolItem.p1_bore == mto_item.p1_bore_in)

                spool_used = spool_used_query.scalar()

                total_used = (direct_used or 0) + (spool_used or 0)
                remaining = max(0, total_required - total_used)

                progress = session.query(MTOProgress).filter(
                    MTOProgress.mto_item_id == mto_item.id
                ).first()

                if not progress:
                    progress = MTOProgress(
                        mto_item_id=mto_item.id, project_id=project_id, line_no=line_no,
                        item_code=mto_item.item_code, description=mto_item.description,
                        unit=mto_item.unit
                    )
                    session.add(progress)

                # --- CHANGE: گرد کردن مقادیر قبل از ذخیره ---
                progress.total_qty = round(total_required, 2)
                progress.used_qty = round(total_used, 2)
                progress.remaining_qty = round(remaining, 2)
                progress.last_updated = datetime.now()

            session.commit()
        except Exception as e:
            session.rollback()
            import traceback
            logging.error(f"خطا در rebuild_mto_progress_for_line: {e}\n{traceback.format_exc()}")
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

    def get_mto_item_by_id(self, mto_item_id: int):
        """یک آیتم MTO را بر اساس شناسه اصلی آن برمی‌گرداند."""
        session = self.get_session()
        try:
            # از session.get برای دسترسی مستقیم و سریع به آیتم با ID استفاده می‌کنیم
            return session.get(MTOItem, mto_item_id)
        except Exception as e:
            logging.error(f"Error fetching MTO item with id {mto_item_id}: {e}")
            return None
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

    def get_enriched_line_progress(self, project_id, line_no, readonly=True):
        """
        داده‌های پیشرفت متریال یک خط را به همراه اطلاعات تکمیلی از MTOItem برمی‌گرداند.
        """
        session = self.get_session()
        try:
            # اگر هیچ رکوردی در جدول پیشرفت نبود، آن را از MTOItem بساز
            if not readonly:
                self.initialize_mto_progress_for_line(project_id, line_no)

            # جوین MTOProgress با MTOItem برای گرفتن اطلاعات بیشتر
            results = session.query(
                MTOProgress,
                MTOItem.p1_bore_in,
                MTOItem.item_type
            ).join(
                MTOItem, MTOProgress.mto_item_id == MTOItem.id
            ).filter(
                MTOProgress.project_id == project_id,
                MTOProgress.line_no == line_no
            ).all()

            progress_data = []
            for item in results:
                progress_record, p1_bore, item_type = item
                progress_data.append({
                    "mto_item_id": progress_record.mto_item_id,
                    "Item Code": progress_record.item_code,
                    "Description": progress_record.description,
                    "Unit": progress_record.unit,
                    "Total Qty": progress_record.total_qty or 0,
                    "Used Qty": progress_record.used_qty or 0,
                    "Remaining Qty": progress_record.remaining_qty or 0,
                    "Bore": p1_bore,
                    "Type": item_type
                })
            return progress_data
        except Exception as e:
            logging.error(f"Error in get_enriched_line_progress for line {line_no}: {e}")
            return []
        finally:
            session.close()

    def initialize_mto_progress_for_line(self, project_id, line_no):
        session = self.get_session()
        try:
            mto_items = session.query(MTOItem).filter(
                MTOItem.project_id == project_id,
                MTOItem.line_no == line_no
            ).all()

            for item in mto_items:
                exists = session.query(MTOProgress).filter(MTOProgress.mto_item_id == item.id).first()
                if not exists:
                    # --- CHANGE: حذف تبدیل واحد ---
                    is_pipe = item.item_type and 'pipe' in item.item_type.lower()
                    if is_pipe:
                        total_required = item.length_m or 0 # دیگر ضرب در ۱۰۰۰ نداریم
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
                        total_qty=round(total_required, 2),
                        used_qty=round(total_used, 2),
                        remaining_qty=round(max(0, total_required - total_used), 2),
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
            lines = session.query(MTOItem.line_no).filter(MTOItem.project_id == project_id).distinct().order_by(
                MTOItem.line_no).all()
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

    # --------------------------------------------------------------------
    # متدهای لازم برای اسپول MIV SPOOL
    # --------------------------------------------------------------------

    def get_mapped_spool_items(self, mto_item_type, p1_bore):
        """
        آیتم‌های اسپول را بر اساس دیکشنری نگاشت برای نوع کامپوننت و سایز (Bore) فیلتر می‌کند.
        تطبیق به‌صورت انعطاف‌پذیر (startswith/contains) انجام می‌شود.
        """
        session = self.get_session()
        try:
            if not mto_item_type:
                return []

            mto_type_upper = str(mto_item_type).upper().strip()
            spool_equivalents = [mto_type_upper]  # همیشه خود آیتم را به عنوان یک احتمال در نظر بگیر

            # 🔹 جستجو در دیکشنری نگاشت برای پیدا کردن معادل‌ها
            for key, aliases in SPOOL_TYPE_MAPPING.items():
                # اگر نوع MTO با یکی از کلیدها یا معادل‌های آن یکی بود
                if mto_type_upper == key or mto_type_upper in aliases:
                    spool_equivalents.extend([key] + list(aliases))
                    break # وقتی گروه درست پیدا شد، از حلقه خارج شو

            # حذف موارد تکراری از لیست معادل‌ها
            spool_equivalents = list(set(spool_equivalents))

            query = session.query(SpoolItem).options(
                joinedload(SpoolItem.spool)
            ).filter(
                # شرط: موجودی بزرگتر از صفر باشد
                (SpoolItem.qty_available > 0.001) | (SpoolItem.length > 0.001),
                # شرط: نوع کامپوننت یکی از موارد موجود در لیست معادل‌ها باشد
                func.upper(SpoolItem.component_type).in_(spool_equivalents)
            )

            # فیلتر بر اساس سایز (Bore) اگر مشخص شده بود
            if p1_bore is not None:
                query = query.filter(SpoolItem.p1_bore == p1_bore)

            return query.all()

        except Exception as e:
            logging.error(f"Error fetching mapped spool items: {e}")
            return []
        finally:
            session.close()

    def register_spool_consumption(self, miv_record_id, spool_consumptions, user="system"):
        """
        لیستی از مصرف‌های اسپول را برای یک MIV مشخص در دیتابیس ثبت می‌کند.
        spool_consumptions: list of dicts -> [{'spool_item_id': id, 'used_qty': qty}]
        """
        session = self.get_session()
        try:
            miv_record = session.query(MIVRecord).get(miv_record_id)
            if not miv_record:
                return False, "رکورد MIV یافت نشد."

            spool_ids_used = set()

            for consumption in spool_consumptions:
                spool_item_id = consumption['spool_item_id']
                used_qty = consumption['used_qty']

                spool_item = session.get(SpoolItem, spool_item_id)
                if not spool_item:
                    raise Exception(f"آیتم اسپول با شناسه {spool_item_id} یافت نشد.")

                if spool_item.qty_available < used_qty:
                    raise Exception(f"موجودی آیتم {spool_item.id} کافی نیست.")

                # ۱. کاهش موجودی از آیتم اسپول
                spool_item.qty_available -= used_qty

                # ۲. ثبت رکورد مصرف در جدول SpoolConsumption
                new_consumption = SpoolConsumption(
                    spool_item_id=spool_item.id,
                    spool_id=spool_item.spool.id,
                    miv_record_id=miv_record_id,
                    used_qty=used_qty,
                    timestamp=datetime.now()
                )
                session.add(new_consumption)
                spool_ids_used.add(str(spool_item.spool.id))

            session.commit()

            self.log_activity(
                user=user,
                action="REGISTER_SPOOL_CONSUMPTION",
                details=f"Spool items consumed for MIV ID {miv_record_id} from Spools: {', '.join(spool_ids_used)}"
            )
            return True, "مصرف اسپول با موفقیت ثبت شد."
        except Exception as e:
            session.rollback()
            logging.error(f"Error in register_spool_consumption: {e}")
            return False, f"خطا در ثبت مصرف اسپول: {e}"
        finally:
            session.close()

    def get_spool_consumptions_for_miv(self, miv_record_id):
        """
        تمام مصرف‌های اسپول ثبت‌شده برای یک MIV خاص را برمی‌گرداند.
        """
        session = self.get_session()
        try:
            return session.query(SpoolConsumption).filter(
                SpoolConsumption.miv_record_id == miv_record_id
            ).options(joinedload(SpoolConsumption.spool_item)).all()
        finally:
            session.close()

    def _get_matching_mto_progress_for_spool(self, session, spool_item, project_id, line_no):
        """
        یک تابع کمکی برای پیدا کردن رکورد MTOProgress متناظر با یک آیتم اسپول.
        تطبیق بر اساس Type و Bore انجام می‌شود.
        """
        mto_item_type = spool_item.component_type
        p1_bore = spool_item.p1_bore

        # پیدا کردن معادل‌های نوع کامپوننت
        mto_type_upper = str(mto_item_type).upper().strip()
        spool_equivalents = [mto_type_upper]
        for key, aliases in SPOOL_TYPE_MAPPING.items():
            if mto_type_upper == key or mto_type_upper in aliases:
                spool_equivalents.extend([key] + list(aliases))
                break
        spool_equivalents = list(set(spool_equivalents))

        # جستجو برای پیدا کردن MTOItem متناظر
        mto_item_query = session.query(MTOItem).filter(
            MTOItem.project_id == project_id,
            MTOItem.line_no == line_no,
            func.upper(MTOItem.item_type).in_(spool_equivalents)
        )
        if p1_bore is not None:
            mto_item_query = mto_item_query.filter(MTOItem.p1_bore_in == p1_bore)

        mto_item = mto_item_query.first()

        if mto_item:
            # پیدا کردن رکورد MTOProgress مربوط به آن MTOItem
            return session.query(MTOProgress).filter(MTOProgress.mto_item_id == mto_item.id).first()

        return None


    # --------------------------------------------------------------------
    # متدهای لازم برای  مدیریت اسپول ها
    # --------------------------------------------------------------------
    def create_spool(self, spool_data: dict, items_data: list[dict]) -> Tuple[bool, str]:
        """
        یک اسپول جدید همراه با آیتم‌هایش ایجاد می‌کند.
        - عملکرد: صحیح. ابتدا وجود اسپول تکراری را چک می‌کند، سپس اسپول و آیتم‌هایش را می‌سازد.
        """
        session = self.get_session()
        try:
            # بررسی تکراری نبودن شناسه اسپول
            existing_spool = session.query(Spool.id).filter(Spool.spool_id == spool_data["spool_id"]).first()
            if existing_spool:
                return False, f"اسپولی با شناسه '{spool_data['spool_id']}' از قبل وجود دارد."

            new_spool = Spool(
                spool_id=spool_data["spool_id"],
                location=spool_data.get("location"),
                # فیلدهای دیگر در صورت نیاز می‌توانند اضافه شوند
            )
            session.add(new_spool)
            session.flush()  # برای گرفتن ID اسپول جدید

            for item in items_data:
                new_item = SpoolItem(
                    spool_id_fk=new_spool.id,
                    component_type=item.get("component_type"),
                    class_angle=item.get("class_angle"),
                    p1_bore=item.get("p1_bore"),
                    p2_bore=item.get("p2_bore"),
                    material=item.get("material"),
                    schedule=item.get("schedule"),
                    length=item.get("length"),
                    qty_available=item.get("qty_available"),
                    item_code=item.get("item_code")
                )
                session.add(new_item)

            session.commit()
            return True, f"اسپول '{new_spool.spool_id}' با موفقیت ساخته شد."
        except Exception as e:
            session.rollback()
            logging.error(f"خطا در ساخت اسپول: {e}")
            return False, f"خطا در ساخت اسپول: {e}"
        finally:
            session.close()

    def update_spool(self, spool_id: str, updated_data: dict, items_data: list[dict]) -> Tuple[bool, str]:
        """
        اطلاعات یک اسپول و تمام آیتم‌های آن را به‌روزرسانی می‌کند.
        - عملکرد: صحیح. اسپول را پیدا کرده، فیلدهایش را آپدیت می‌کند، آیتم‌های قدیمی را کاملاً پاک کرده و آیتم‌های جدید را جایگزین می‌کند.
        """
        session = self.get_session()
        try:
            spool = session.query(Spool).filter(Spool.spool_id == spool_id).first()
            if not spool:
                return False, "اسپول یافت نشد."

            # آپدیت فیلدهای اصلی اسپول (مانند لوکیشن)
            for key, value in updated_data.items():
                if hasattr(spool, key):
                    setattr(spool, key, value)

            # پاک کردن آیتم‌های قدیمی
            session.query(SpoolItem).filter(SpoolItem.spool_id_fk == spool.id).delete()
            session.flush()

            # افزودن آیتم‌های جدید
            for item in items_data:
                new_item = SpoolItem(
                    spool_id_fk=spool.id,
                    component_type=item.get("component_type"),
                    class_angle=item.get("class_angle"),
                    p1_bore=item.get("p1_bore"),
                    p2_bore=item.get("p2_bore"),
                    material=item.get("material"),
                    schedule=item.get("schedule"),
                    length=item.get("length"),
                    qty_available=item.get("qty_available"),
                    item_code=item.get("item_code")
                )
                session.add(new_item)

            session.commit()
            return True, f"اسپول '{spool_id}' با موفقیت ویرایش شد."
        except Exception as e:
            session.rollback()
            logging.error(f"خطا در آپدیت اسپول: {e}")
            return False, f"خطا در آپدیت اسپول: {e}"
        finally:
            session.close()

    def generate_next_spool_id(self) -> str:
        """
        یک شناسه جدید برای اسپول بر اساس آخرین شناسه موجود تولید می‌کند.
        - عملکرد: صحیح و قوی. با استفاده از regex عدد را از انتهای آخرین شناسه استخراج و یکی به آن اضافه می‌کند.
        """
        session = self.get_session()
        try:
            # برای پیدا کردن آخرین رکورد، بر اساس ID که همیشه صعودی است مرتب می‌کنیم
            last_spool = session.query(Spool).order_by(Spool.id.desc()).first()
            if not last_spool:
                return "S001"

            last_id = last_spool.spool_id
            # پیدا کردن تمام اعداد در شناسه
            numeric_parts = re.findall(r'\d+', last_id)

            if numeric_parts:
                # آخرین عدد پیدا شده را یکی اضافه کن
                next_num = int(numeric_parts[-1]) + 1
            else:
                # اگر هیچ عددی در شناسه نبود، از ID خود رکورد استفاده کن
                next_num = last_spool.id + 1

            # فرمت‌دهی به صورت سه رقمی (e.g., S001, S012, S123)
            return f"S{next_num:03d}"
        except Exception as e:
            logging.error(f"Error generating next spool ID: {e}")
            return f"S_ERR_{datetime.now().microsecond}"
        finally:
            session.close()

    def get_spool_by_id(self, spool_id: str):
        """
        یک اسپول را به همراه تمام آیتم‌های مرتبط با آن برمی‌گرداند.
        - عملکرد: صحیح. استفاده از joinedload باعث می‌شود آیتم‌ها همراه با خود اسپول در یک کوئری خوانده شوند که بهینه است.
        """
        session = self.get_session()
        try:
            spool = session.query(Spool).filter(Spool.spool_id == spool_id).options(joinedload(Spool.items)).first()
            return spool
        finally:
            session.close()  # بستن سشن در هر صورت ضروری است

    def export_spool_data_to_excel(self, file_path: str) -> Tuple[bool, str]:
        """
        داده‌های جداول Spool, SpoolItem و SpoolConsumption را به شیت‌های مجزا در یک فایل اکسل خروجی می‌دهد.
        - عملکرد: صحیح. از کتابخانه پانداز برای خواندن مستقیم از کوئری و نوشتن در اکسل استفاده می‌کند که کارآمد است.
        """
        session = self.get_session()
        try:
            tables_to_export = {
                "Spools": Spool,
                "SpoolItems": SpoolItem,
                "SpoolConsumptions": SpoolConsumption
            }
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                for sheet_name, model_class in tables_to_export.items():
                    query = session.query(model_class)
                    df = pd.read_sql(query.statement, session.bind)
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

            self.log_activity("system", "EXPORT_TO_EXCEL", f"Spool data exported to {file_path}")
            return True, f"داده‌ها با موفقیت در فایل {file_path} ذخیره شدند."
        except Exception as e:
            logging.error(f"خطا در خروجی گرفتن اکسل: {e}")
            return False, f"خطا در ایجاد فایل اکسل: {e}"
        finally:
            session.close()

    def get_all_spool_ids(self) -> list[str]:
        """
        لیستی از تمام شناسه‌های اسپول موجود را برمی‌گرداند.
        - عملکرد: صحیح. کوئری ساده برای گرفتن تمام شناسه‌ها و تبدیل نتیجه به لیست رشته‌ها.
        """
        session = self.get_session()
        try:
            results = session.query(Spool.spool_id).order_by(Spool.spool_id).all()
            return [item[0] for item in results]
        except Exception as e:
            logging.error(f"Error fetching all spool IDs: {e}")
            return []
        finally:
            session.close()

