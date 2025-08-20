# main_app_pyqt.py

import sys
import webbrowser
import subprocess
import os
from functools import partial

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QFormLayout,
    QLabel, QComboBox, QPushButton, QTextEdit, QFrame, QMessageBox, QLineEdit,
    QTableWidget, QTableWidgetItem, QHeaderView, QDialog, QDialogButtonBox, QDoubleSpinBox, QSplitter,
    QCompleter, QInputDialog, QFileDialog, QGroupBox
)

from PyQt6.QtGui import QFont, QColor
from PyQt6.QtCore import Qt, QStringListModel

# برای نمایش نمودار در PyQt6
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

# فرض بر این است که این دو فایل در کنار این اسکریپت قرار دارند
from data_manager import DataManager
from models import Project, MTOItem, MIVRecord, Spool, SpoolItem  # برای type hinting

import sys, traceback

class SpoolManagerDialog(QDialog):
    def __init__(self, dm: DataManager, parent=None):
        super().__init__(parent)
        self.dm = dm
        self.setWindowTitle("مدیریت اسپول‌ها")
        self.setMinimumSize(1000, 700)

        # ------------------- چیدمان اصلی -------------------
        layout = QVBoxLayout(self)

        # ------------------- بخش اطلاعات و بارگذاری اسپول -------------------
        top_groupbox = QGroupBox("اطلاعات اسپول")
        top_layout = QHBoxLayout()

        form_layout = QFormLayout()
        self.spool_id_entry = QLineEdit()
        self.spool_id_entry.setPlaceholderText("شناسه اسپول را وارد یا انتخاب کنید...")

        # --- NEW: فیلد ورودی برای لوکیشن ---
        self.location_entry = QLineEdit()
        self.location_entry.setPlaceholderText("محل قرارگیری اسپول...")

        form_layout.addRow("Spool ID:", self.spool_id_entry)
        form_layout.addRow("Location:", self.location_entry)

        self.load_btn = QPushButton("بارگذاری اسپول")
        self.new_btn = QPushButton("ایجاد اسپول جدید")

        top_layout.addLayout(form_layout, stretch=2)
        top_layout.addWidget(self.load_btn)
        top_layout.addWidget(self.new_btn)
        top_groupbox.setLayout(top_layout)
        layout.addWidget(top_groupbox)

        # --- NEW: اضافه کردن تکمیل‌کننده خودکار (Completer) برای Spool ID ---
        self.setup_spool_id_completer()

        # ------------------- جدول آیتم‌های اسپول -------------------
        self.table = QTableWidget()
        # --- NEW: تعداد ستون‌ها به 9 افزایش یافت تا Item Code اضافه شود ---
        self.table.setColumnCount(9)
        self.table.setHorizontalHeaderLabels([
            "Component Type", "Class/Angle", "Bore1", "Bore2",
            "Material", "Schedule", "Length", "Qty Available", "Item Code"
        ])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.table)

        # ------------------- دکمه‌های کنترلی -------------------
        btns_layout = QHBoxLayout()
        self.add_row_btn = QPushButton("➕ افزودن ردیف")
        self.remove_row_btn = QPushButton("➖ حذف ردیف")

        # --- NEW: دکمه خروجی اکسل اضافه شد ---
        self.export_btn = QPushButton("خروجی اکسل")

        self.save_btn = QPushButton("💾 ذخیره تغییرات")
        self.close_btn = QPushButton("بستن")

        btns_layout.addWidget(self.add_row_btn)
        btns_layout.addWidget(self.remove_row_btn)
        btns_layout.addStretch()
        btns_layout.addWidget(self.export_btn)  # دکمه اکسل
        btns_layout.addWidget(self.save_btn)
        btns_layout.addWidget(self.close_btn)
        layout.addLayout(btns_layout)

        # ------------------- اتصال سیگنال‌ها به توابع -------------------
        self.load_btn.clicked.connect(self.load_spool)
        self.new_btn.clicked.connect(self.new_spool)
        self.add_row_btn.clicked.connect(self.add_row)
        self.remove_row_btn.clicked.connect(self.remove_row)
        self.save_btn.clicked.connect(self.save_changes)
        self.export_btn.clicked.connect(self.handle_export_to_excel)  # اتصال دکمه اکسل
        self.close_btn.clicked.connect(self.close)

        # ------------------- وضعیت جاری برنامه -------------------
        self.current_spool_id = None
        self.is_new_spool = False  # برای تفکیک بین حالت ایجاد و ویرایش

    def setup_spool_id_completer(self):
        """لیست شناسه‌های اسپول را از دیتابیس گرفته و به ورودی اضافه می‌کند."""
        try:
            spool_ids = self.dm.get_all_spool_ids()  # این متد باید در DataManager ساخته شود
            model = QStringListModel()
            model.setStringList(spool_ids)

            completer = QCompleter()
            completer.setModel(model)
            completer.setCaseSensitivity(0)  # Not case sensitive
            self.spool_id_entry.setCompleter(completer)
        except Exception as e:
            print(f"Failed to setup completer: {e}")

    def populate_table(self, items):
        """جدول را با آیتم‌های اسپول پر می‌کند"""
        try:
            self.table.setRowCount(len(items))
            for row, item in enumerate(items):
                self.table.setItem(row, 0, QTableWidgetItem(item.component_type or ""))
                self.table.setItem(row, 1, QTableWidgetItem(item.class_angle or ""))
                self.table.setItem(row, 2, QTableWidgetItem(str(item.p1_bore) if item.p1_bore is not None else ""))
                self.table.setItem(row, 3, QTableWidgetItem(str(item.p2_bore) if item.p2_bore is not None else ""))
                self.table.setItem(row, 4, QTableWidgetItem(item.material or ""))
                self.table.setItem(row, 5, QTableWidgetItem(item.schedule or ""))
                self.table.setItem(row, 6, QTableWidgetItem(str(item.length) if item.length is not None else ""))
                self.table.setItem(row, 7, QTableWidgetItem(str(item.qty_available) if item.qty_available is not None else ""))
        except Exception as e:
            self.show_msg("خطا", "save_changes failed", detailed=str(e), icon=QMessageBox.Icon.Critical)


    def add_row(self):
        self.table.insertRow(self.table.rowCount())

    def remove_row(self):
        row = self.table.currentRow()
        if row >= 0:
            self.table.removeRow(row)

    def load_spool(self):
        try:
            spool_id = self.spool_id_entry.text().strip()
            spool = self.dm.get_spool_by_id(spool_id)
            if not spool:
                QMessageBox.warning(self, "خطا", "اسپول یافت نشد.")
                return
            self.current_spool_id = spool.spool_id
            self.populate_table(spool.items)
        except Exception as e:
            self.show_msg("خطا", "save_changes failed", detailed=str(e), icon=QMessageBox.Icon.Critical)

    def new_spool(self):
        try:
            self.current_spool_id = None
            self.table.setRowCount(0)
            new_id = self.dm.generate_next_spool_id()
            self.spool_id_entry.setText(new_id)
            QMessageBox.information(self, "اطلاع", f"اسپول جدید با ID {new_id} آماده ورود اطلاعات است.")
        except Exception as e:
            self.show_msg("خطا", "save_changes failed", detailed=str(e), icon=QMessageBox.Icon.Critical)


    def save_changes(self):  # # ذخیره‌سازی داده‌های جدول در دیتابیس
        try:
            def safe_float(txt):  # # مبدل امن متن به عدد اعشاری یا None
                if txt is None:
                    return None
                s = str(txt).strip()
                if s == "":
                    return None
                try:
                    return float(s)
                except Exception:
                    return None

            items_data = []
            for r in range(self.table.rowCount()):
                row = {
                    "component_type": self.table.item(r, 0).text().strip() if self.table.item(r, 0) else None,
                    "class_angle": self.table.item(r, 1).text().strip() if self.table.item(r, 1) else None,
                    "p1_bore": safe_float(self.table.item(r, 2).text() if self.table.item(r, 2) else None),
                    "p2_bore": safe_float(self.table.item(r, 3).text() if self.table.item(r, 3) else None),
                    "material": self.table.item(r, 4).text().strip() if self.table.item(r, 4) else None,
                    "schedule": self.table.item(r, 5).text().strip() if self.table.item(r, 5) else None,
                    "thickness": safe_float(self.table.item(r, 6).text() if self.table.item(r, 6) else None),
                    "length": safe_float(self.table.item(r, 7).text() if self.table.item(r, 7) else None),
                    "qty_available": safe_float(self.table.item(r, 8).text() if self.table.item(r, 8) else None),
                    "item_code": self.table.item(r, 9).text().strip() if self.table.item(r, 9) else None,
                }
                items_data.append(row)

            # تعیین لوکیشن از فیلد بالایی یا ستون آخر جدول
            loc_from_field = self.location_entry.text().strip()
            loc_from_table = None
            if self.table.rowCount() > 0 and self.table.item(0, 10):
                loc_from_table = self.table.item(0, 10).text().strip()
            final_location = loc_from_field or loc_from_table or None

            spool_id = self.spool_id_entry.text().strip()
            if not spool_id:
                self.show_msg("هشدار", "Spool ID الزامی است.", icon=QMessageBox.Icon.Warning)
                return

            spool_data = {
                "spool_id": spool_id,
                "location": final_location
            }

            if self.current_spool_id:  # ویرایش
                success, msg = self.dm.update_spool(self.current_spool_id, spool_data, items_data)
            else:  # ایجاد
                success, msg = self.dm.create_spool(spool_data, items_data)
                if success:
                    self.current_spool_id = spool_id

            if success:
                self.show_msg("موفق", msg)
            else:
                self.show_msg("خطا", msg, icon=QMessageBox.Icon.Critical)

        except Exception as e:
            self.show_msg("خطا", "save_changes با خطا مواجه شد.", detailed=str(e), icon=QMessageBox.Icon.Critical)

    def handle_export_to_excel(self):
        try:
            path, _ = QFileDialog.getSaveFileName(self, "ذخیره فایل اکسل", "Spool_Data.xlsx", "Excel Files (*.xlsx)")
            if not path:
                return
            ok, message = self.dm.export_spool_data_to_excel(path)  # ← اصلاح شد
            icon = QMessageBox.Icon.Information if ok else QMessageBox.Icon.Critical
            self.show_msg("خروجی اکسل", message, icon=icon)
        except Exception as e:
            self.show_msg("خطا", "Export به اکسل با خطا مواجه شد.", detailed=str(e), icon=QMessageBox.Icon.Critical)

    def show_msg(self, title, text, detailed=None, icon=QMessageBox.Icon.Information):
        box = QMessageBox(self)
        box.setIcon(icon)
        box.setWindowTitle(title)
        box.setText(text)
        if detailed:  # متن کامل خطا یا جزییات
            box.setDetailedText(detailed)

        # ✨ این خط باعث میشه متن پیام قابل انتخاب/کپی باشه
        box.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse |
            Qt.TextInteractionFlag.TextSelectableByKeyboard
        )

        box.exec()


class SpoolSelectionDialog(QDialog):
    def __init__(self, matching_items: list[SpoolItem], parent=None):
        super().__init__(parent)
        self.setWindowTitle("انتخاب آیتم از انبار اسپول")
        self.setMinimumSize(800, 400)

        self.selected_data = []
        self.items = matching_items

        layout = QVBoxLayout(self)

        info_label = QLabel("مقدار مورد نیاز از هر آیتم را در ستون 'مقدار مصرف' وارد کنید.")
        layout.addWidget(info_label)

        self.table = QTableWidget()
        self.table.setColumnCount(7)
        self.table.setHorizontalHeaderLabels([
            "ID", "Spool ID", "Component Type", "Bore", "Material", "موجودی", "مقدار مصرف"
        ])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.table)

        self.populate_table()

        self.buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.buttons.accepted.connect(self.accept_data)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def populate_table(self):
        self.table.setRowCount(len(self.items))
        for row, item in enumerate(self.items):
            self.table.setItem(row, 0, QTableWidgetItem(str(item.id)))
            self.table.setItem(row, 1, QTableWidgetItem(str(item.spool.spool_id)))
            self.table.setItem(row, 2, QTableWidgetItem(item.component_type))
            self.table.setItem(row, 3, QTableWidgetItem(str(item.p1_bore)))
            self.table.setItem(row, 4, QTableWidgetItem(item.material))

            # --- منطق جدید برای نمایش موجودی ---
            if "PIPE" in (item.component_type or "").upper():
                available_qty = item.length or 0
            else:
                available_qty = item.qty_available or 0

            self.table.setItem(row, 5, QTableWidgetItem(str(available_qty)))

            spin_box = QDoubleSpinBox()
            spin_box.setRange(0, available_qty)
            spin_box.setDecimals(3)
            self.table.setCellWidget(row, 6, spin_box)

            for col in range(6):
                self.table.item(row, col).setFlags(self.table.item(row, col).flags() & ~Qt.ItemFlag.ItemIsEditable)

    def accept_data(self):
        self.selected_data = []
        for row in range(self.table.rowCount()):
            spin_box = self.table.cellWidget(row, 6)
            used_qty = spin_box.value()

            if used_qty > 0:
                spool_item_id = int(self.table.item(row, 0).text())
                self.selected_data.append({
                    "spool_item_id": spool_item_id,
                    "used_qty": used_qty
                })#
        self.accept()

    def get_selected_data(self):
        return self.selected_data


class MTOConsumptionDialog(QDialog):
    def __init__(self, dm: DataManager, project_id: int, line_no: str, miv_record_id: int = None, parent=None):
        super().__init__(parent)
        self.dm = dm
        self.project_id = project_id
        self.line_no = line_no
        self.miv_record_id = miv_record_id

        # Data storage
        self.consumed_data = []  # For direct MTO consumption
        self.spool_consumption_data = []  # For spool consumption
        self.spool_selections = {}  # Internal UI mapping: {row_index: [list of spool selections]}

        self.existing_consumptions = {}
        # We don't need to fetch existing spool consumptions as the logic
        # is handled by the data manager during the update.

        self.setWindowTitle(f"مدیریت مصرف برای خط: {self.line_no}")
        self.setMinimumSize(1200, 600)

        if self.miv_record_id:
            self.setWindowTitle(f"ویرایش آیتم‌های MIV ID: {self.miv_record_id}")
            self.existing_consumptions = self.dm.get_consumptions_for_miv(self.miv_record_id)

        layout = QVBoxLayout(self)
        info_label = QLabel(
            "مقدار مصرف مستقیم را وارد کنید یا از دکمه 'انتخاب اسپول' برای برداشت از انبار اسپول استفاده نمایید.")
        layout.addWidget(info_label)

        self.table = QTableWidget()
        self.table.setColumnCount(13)
        self.table.setHorizontalHeaderLabels([
            # MTO Info
            "Item Code", "Description", "Total Qty", "Used (All)", "Remaining", "Unit",
            # New MTO Details
            "Bore", "Type",
            # Consumption for this MIV
            "مصرف مستقیم",
            # Spool Info
            "انتخاب اسپول", "Spool ID", "Qty from Spool", "Spool Remaining"
        ])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.resizeColumnsToContents()
        layout.addWidget(self.table)

        self.populate_table()

        self.buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.buttons.accepted.connect(self.accept_data)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def populate_table(self):
        self.progress_data = self.dm.get_enriched_line_progress(self.project_id, self.line_no, readonly=False)
        self.table.setRowCount(len(self.progress_data))

        for row_idx, item in enumerate(self.progress_data):
            mto_item_id = item["mto_item_id"]

            # Populate MTO columns (0-7)
            self.table.setItem(row_idx, 0, QTableWidgetItem(item["Item Code"] or ""))
            self.table.setItem(row_idx, 1, QTableWidgetItem(item["Description"] or ""))
            self.table.setItem(row_idx, 2, QTableWidgetItem(str(item["Total Qty"])))
            self.table.setItem(row_idx, 3, QTableWidgetItem(str(item["Used Qty"])))
            remaining_qty = item["Remaining Qty"] or 0
            self.table.setItem(row_idx, 4, QTableWidgetItem(str(remaining_qty)))
            self.table.setItem(row_idx, 5, QTableWidgetItem(item["Unit"] or ""))
            self.table.setItem(row_idx, 6, QTableWidgetItem(str(item.get("Bore") or "")))
            self.table.setItem(row_idx, 7, QTableWidgetItem(item.get("Type") or ""))

            # Get total usage for *this MIV* to set initial state
            # In edit mode, this value is the sum of direct + spool for this MIV
            current_miv_total_usage = self.existing_consumptions.get(mto_item_id, 0)

            # SpinBox for direct consumption
            spin_box = QDoubleSpinBox()
            max_val = remaining_qty + current_miv_total_usage
            spin_box.setRange(0, max_val)
            spin_box.setDecimals(3)
            # In edit mode, we assume the initial value is all direct for simplicity.
            # The user will have to re-select from spools if they wish to change it.
            spin_box.setValue(current_miv_total_usage)
            self.table.setCellWidget(row_idx, 8, spin_box)

            # Button for spool selection
            spool_btn = QPushButton("انتخاب...")
            spool_btn.clicked.connect(partial(self.handle_spool_selection, row_idx))
            self.table.setCellWidget(row_idx, 9, spool_btn)

            # Placeholders for Spool info
            for col in [10, 11, 12]:
                self.table.setItem(row_idx, col, QTableWidgetItem(""))

            if max_val <= 0:
                spin_box.setEnabled(False)
                spool_btn.setEnabled(False)

            # Make info columns read-only
            for col in list(range(8)) + [10, 11, 12]:
                item_widget = self.table.item(row_idx, col)
                if item_widget:
                    item_widget.setFlags(item_widget.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self.table.resizeColumnsToContents()

    def handle_spool_selection(self, row_idx):
        item_data = self.progress_data[row_idx]
        # دوباره اطلاعات تایپ و سایز را از ردیف MTO می‌خوانیم
        item_type = item_data.get("Type")
        p1_bore = item_data.get("Bore")

        if not item_type:
            self.parent().show_message("هشدار", "نوع آیتم (Type) برای این ردیف MTO مشخص نشده است.", "warning")
            # اگر تایپ مشخص نباشد، برای جلوگیری از خطا، تابع را متوقف می‌کنیم
            return

        # فراخوانی تابع جدید که از دیکشنری نگاشت استفاده می‌کند
        matching_items = self.dm.get_mapped_spool_items(item_type, p1_bore)

        if not matching_items:
            self.parent().show_message("اطلاعات", f"هیچ اسپول سازگار برای نوع '{item_type}' و سایز '{p1_bore}' یافت نشد.", "info")
            return

        dialog = SpoolSelectionDialog(matching_items, self)
        if dialog.exec():
            selected_spools = dialog.get_selected_data()
            self.spool_selections[row_idx] = selected_spools
            self.update_row_after_spool_selection(row_idx)

    def update_row_after_spool_selection(self, row_idx):
        selections = self.spool_selections.get(row_idx, [])
        if not selections:
            self.table.item(row_idx, 10).setText("")
            self.table.item(row_idx, 11).setText("")
            self.table.item(row_idx, 12).setText("")
            return

        total_spool_qty = sum(s['used_qty'] for s in selections)

        session = self.dm.get_session()
        try:
            first_selection = selections[0]
            spool_item = session.query(SpoolItem).get(first_selection['spool_item_id'])
            spool_id_text = str(spool_item.spool.spool_id)
            if len(selections) > 1:
                spool_id_text += f" (+{len(selections) - 1} more)"

            self.table.item(row_idx, 10).setText(spool_id_text)  # Spool ID
            self.table.item(row_idx, 11).setText(str(total_spool_qty))  # Qty from Spool
            self.table.item(row_idx, 12).setText(str(spool_item.qty_available - first_selection['used_qty']))
        finally:
            session.close()

        item_data = self.progress_data[row_idx]
        remaining_qty = item_data["Remaining Qty"] or 0
        current_miv_usage = self.existing_consumptions.get(item_data["mto_item_id"], 0)

        spin_box = self.table.cellWidget(row_idx, 8)
        new_max = (remaining_qty + current_miv_usage) - total_spool_qty
        spin_box.setRange(0, max(0, new_max))
        if spin_box.value() > new_max:
            spin_box.setValue(max(0, new_max))

    def accept_data(self):
        self.consumed_data = []
        self.spool_consumption_data = []

        for row_idx, item in enumerate(self.progress_data):
            total_consumed_for_item = 0

            spin_box = self.table.cellWidget(row_idx, 8)
            direct_qty = spin_box.value()
            total_consumed_for_item += direct_qty

            spool_selections = self.spool_selections.get(row_idx, [])
            if spool_selections:
                qty_from_spools = sum(s['used_qty'] for s in spool_selections)
                total_consumed_for_item += qty_from_spools
                self.spool_consumption_data.extend(spool_selections)

            if total_consumed_for_item > 0:
                self.consumed_data.append({
                    "mto_item_id": item["mto_item_id"],
                    "used_qty": total_consumed_for_item,
                    "item_code": item["Item Code"],
                    "description": item["Description"],
                    "unit": item["Unit"]
                })

        super().accept()

    def get_data(self):
        return self.consumed_data, self.spool_consumption_data


# --- پنجره اصلی برنامه ---
class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("مدیریت MIV - نسخه PyQt6")
        self.setGeometry(100, 100, 1200, 800)

        self.dm = DataManager(db_path="miv_registry.db")
        self.current_project: Project | None = None
        self.current_user = os.getlogin()  # گرفتن نام کاربری سیستم
        self.suggestion_data = []  # 🔹 برای نگهداری داده‌های کامل پیشنهادها

        self.setup_ui()
        self.connect_signals()
        self.populate_project_combo()
        QApplication.instance().aboutToQuit.connect(self.cleanup_processes)
        self.dashboard_password = "hossein"  # 🔒 رمز موقت

    def setup_ui(self):
        """متد اصلی برای ساخت و چیدمان تمام ویجت‌ها."""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)

        # --- اسپلیتر برای تقسیم صفحه ---
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # --- پنل سمت چپ (فرم و داشبورد) ---
        left_panel = QFrame()
        left_layout = QVBoxLayout(left_panel)

        # فرم ثبت
        reg_form_frame = QFrame()
        reg_form_frame.setFrameShape(QFrame.Shape.StyledPanel)
        self.create_registration_form(reg_form_frame)

        # داشبورد
        dashboard_frame = QFrame()
        dashboard_frame.setFrameShape(QFrame.Shape.StyledPanel)
        self.create_dashboard(dashboard_frame)

        left_layout.addWidget(reg_form_frame)
        left_layout.addWidget(dashboard_frame, 1)

        # --- پنل سمت راست (جستجو و کنسول) ---
        right_panel = QFrame()
        right_layout = QVBoxLayout(right_panel)

        # بخش جستجو
        search_frame = QFrame()
        search_frame.setFrameShape(QFrame.Shape.StyledPanel)
        self.create_search_box(search_frame)

        # کنسول
        console_frame = QFrame()
        console_frame.setFrameShape(QFrame.Shape.StyledPanel)
        self.create_console(console_frame)

        right_layout.addWidget(search_frame)
        right_layout.addWidget(console_frame, 1)

        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setSizes([500, 600])  # سایز اولیه پنل‌ها

        main_layout.addWidget(splitter)

    def create_registration_form(self, parent_widget):
        layout = QVBoxLayout(parent_widget)
        layout.addWidget(QLabel("<h2>ثبت رکورد MIV جدید</h2>"))

        form_layout = QFormLayout()
        self.entries = {}
        fields = ["Line No", "MIV Tag", "Location", "Status", "Registered For"]
        for field in fields:
            self.entries[field] = QLineEdit()
            form_layout.addRow(f"{field}:", self.entries[field])

        # 🔹 راه‌اندازی Completer برای فیلد Line No
        self.line_completer_model = QStringListModel()
        self.line_completer = QCompleter(self.line_completer_model, self)
        self.line_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.line_completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self.entries["Line No"].setCompleter(self.line_completer)

        self.register_btn = QPushButton("ثبت رکورد")
        layout.addLayout(form_layout)
        layout.addWidget(self.register_btn)
        layout.addStretch()

    def create_dashboard(self, parent_widget):
        layout = QVBoxLayout(parent_widget)
        layout.addWidget(QLabel("<h3>داشبورد پیشرفت خط</h3>"))
        # نمودار پای‌چارت اصلی
        self.fig = Figure(figsize=(5, 4), dpi=100)
        self.canvas = FigureCanvas(self.fig)
        layout.addWidget(self.canvas)

        self.dashboard_ax = self.fig.add_subplot(111)
        self.dashboard_ax.text(0.5, 0.5, "Enter the line number", ha='center', va='center')

        self.canvas.draw()

        # دکمه نمایش جزئیات
        self.details_btn = QPushButton("نمایش جزئیات خط")
        self.details_btn.clicked.connect(self.show_line_details)
        layout.addWidget(self.details_btn)

    def create_search_box(self, parent_widget):
        layout = QVBoxLayout(parent_widget)
        layout.addWidget(QLabel("<h3>جستجو و نمایش</h3>"))

        search_layout = QHBoxLayout()
        self.search_entry = QLineEdit()
        self.search_entry.setPlaceholderText("بخشی از شماره خط را برای جستجو و پیشنهاد وارد کنید...")
        self.search_btn = QPushButton("جستجو")

        # 🔹 استفاده مجدد از همان Completer برای فیلد جستجو
        self.search_entry.setCompleter(self.line_completer)

        search_layout.addWidget(self.search_entry)
        search_layout.addWidget(self.search_btn)
        layout.addLayout(search_layout)

    def create_console(self, parent_widget):
        layout = QVBoxLayout(parent_widget)
        self.project_combo = QComboBox()
        self.load_project_btn = QPushButton("بارگذاری پروژه")

        project_layout = QHBoxLayout()
        project_layout.addWidget(QLabel("پروژه فعال:"))
        project_layout.addWidget(self.project_combo, 1)
        project_layout.addWidget(self.load_project_btn)

        self.manage_spool_btn = QPushButton("مدیریت اسپول‌ها")
        layout.addWidget(self.manage_spool_btn)

        self.console_output = QTextEdit()
        self.console_output.setReadOnly(True)
        self.console_output.setFont(QFont("Consolas", 11))
        self.console_output.setStyleSheet("background-color: #2b2b2b; color: #f8f8f2;")

        layout.addLayout(project_layout)
        layout.addWidget(self.console_output, 1)

    def connect_signals(self):
        self.load_project_btn.clicked.connect(self.load_project)
        self.register_btn.clicked.connect(self.handle_registration)
        self.search_btn.clicked.connect(self.handle_search)

        self.entries["Line No"].textChanged.connect(self.update_suggestions)
        self.search_entry.textChanged.connect(self.update_suggestions)

        # 🔹 سیگنال کلیدی برای انتخاب یک آیتم از لیست پیشنهادها
        self.line_completer.activated.connect(self.on_suggestion_selected)

        self.entries["Line No"].textChanged.connect(self.update_line_dashboard)

        self.manage_spool_btn.clicked.connect(self.open_spool_manager)

    def populate_project_combo(self):
        self.project_combo.clear()
        try:
            projects = self.dm.get_all_projects()
            if not projects:
                self.project_combo.addItem("هیچ پروژه‌ای یافت نشد", userData=None)
            else:
                # 🔹 یک آیتم "همه پروژه‌ها" برای حالت اولیه اضافه می‌کنیم
                self.project_combo.addItem("همه پروژه‌ها", userData=None)
                for proj in projects:
                    self.project_combo.addItem(proj.name, userData=proj)
        except Exception as e:
            self.log_to_console(f"خطا در بارگذاری پروژه‌ها: {e}", "error")

    def load_project(self):
        selected_index = self.project_combo.currentIndex()
        if selected_index == -1: return

        project_data = self.project_combo.itemData(selected_index)
        self.current_project = project_data

        if self.current_project:
            self.log_to_console(f"پروژه '{self.current_project.name}' با موفقیت بارگذاری شد.", "success")
        else:
            # اگر "همه پروژه‌ها" انتخاب شود
            self.log_to_console("حالت جستجوی سراسری فعال است. یک خط را جستجو کنید.", "info")

    def update_suggestions(self, text):
        """
        مدل Completer را با پیشنهادهای سراسری به‌روز می‌کند.
        """
        # 🔹 دیگر نیازی به انتخاب پروژه نیست
        if len(text) < 2:
            self.line_completer_model.setStringList([])
            return

        # ۱. دریافت داده‌های کامل از دیتابیس
        self.suggestion_data = self.dm.get_line_no_suggestions(text)

        # ۲. استخراج متن نمایشی برای Completer
        display_list = [item['display'] for item in self.suggestion_data]
        self.line_completer_model.setStringList(display_list)

    def on_suggestion_selected(self, selected_display_text):
        """
        وقتی کاربر یک پیشنهاد را انتخاب می‌کند، این متد فراخوانی می‌شود.
        """
        # ۱. پیدا کردن اطلاعات کامل پیشنهاد انتخاب‌شده
        selected_item = next((item for item in self.suggestion_data if item['display'] == selected_display_text), None)

        if not selected_item:
            return

        project_name = selected_item['project_name']
        line_no = selected_item['line_no']

        index = self.project_combo.findText(project_name, Qt.MatchFlag.MatchFixedString)
        if index >= 0:
            self.project_combo.setCurrentIndex(index)
            # فراخوانی load_project برای به‌روزرسانی self.current_project
            self.load_project()

        # با QApplication.focusWidget() می‌فهمیم کدام فیلد فعال بوده است
        focused_widget = QApplication.focusWidget()
        if isinstance(focused_widget, QLineEdit):
            focused_widget.setText(line_no)
            # آپدیت داشبورد با خط جدید
            self.update_line_dashboard()

    def handle_registration(self):
        if not self.current_project:
            self.show_message("خطا", "لطفاً ابتدا یک پروژه را بارگذاری کنید.", "warning")
            return

        # ۱. جمع‌آوری داده از فرم
        form_data = {field: widget.text().strip() for field, widget in self.entries.items()}
        form_data["Registered By"] = self.current_user
        form_data["Complete"] = False
        form_data["Comment"] = ""

        # ۲. اعتبارسنجی
        if not form_data["Line No"] or not form_data["MIV Tag"]:
            self.show_message("خطا", "فیلدهای Line No و MIV Tag اجباری هستند.", "warning")
            return

        if self.dm.is_duplicate_miv_tag(form_data["MIV Tag"], self.current_project.id):
            self.show_message("خطا", f"تگ '{form_data['MIV Tag']}' در این پروژه تکراری است.", "error")
            return

        # ✅ ۳. اطمینان از اینکه MTOProgress برای خط وجود دارد
        self.dm.initialize_mto_progress_for_line(self.current_project.id, form_data["Line No"])

        # ۴. انتخاب مصرف
        dialog = MTOConsumptionDialog(self.dm, self.current_project.id, form_data["Line No"], parent=self)

        if dialog.exec():
            consumed_items, spool_items = dialog.get_data()
            if not consumed_items and not spool_items:
                self.log_to_console("ثبت رکورد لغو شد چون هیچ آیتمی مصرف نشده بود.", "warning")
                return

            # ساخت کامنت
            comment_parts = [
                f"{item['used_qty']} * {(item.get('item_code') or item['description'])}"
                for item in consumed_items
            ]
            form_data["Comment"] = ", ".join(comment_parts)

            # ثبت نهایی
            success, msg = self.dm.register_miv_record(self.current_project.id, form_data, consumed_items, spool_items)
            if success:
                self.log_to_console(msg, "success")
                self.update_line_dashboard()
                self.entries["MIV Tag"].clear()
                self.entries["Location"].clear()
                self.entries["Status"].clear()
            else:
                self.log_to_console(msg, "error")

    def handle_search(self):
        if not self.current_project:
            self.show_message("خطا", "لطفاً ابتدا یک پروژه را بارگذاری کنید.", "warning")
            return

        line_no = self.search_entry.text().strip()
        if not line_no:
            self.show_message("خطا", "لطفاً شماره خط برای جستجو را وارد کنید.", "warning")
            return

        records = self.dm.search_miv_by_line_no(self.current_project.id, line_no)

        if not records:
            self.show_message("نتیجه", f"هیچ رکوردی برای خط '{line_no}' یافت نشد.", "info")
            return

        dlg = QDialog(self)
        dlg.setWindowTitle(f"نتایج جستجو - خط {line_no}")
        dlg.resize(950, 450)
        layout = QVBoxLayout(dlg)

        table = QTableWidget()
        table.setColumnCount(8)
        table.setHorizontalHeaderLabels([
            "ID", "MIV Tag", "Location", "Status", "Comment",
            "Registered For", "Registered By", "Last Updated"
        ])
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)

        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        table.setRowCount(len(records))

        for row, rec in enumerate(records):
            table.setItem(row, 0, QTableWidgetItem(str(rec.id)))
            table.setItem(row, 1, QTableWidgetItem(rec.miv_tag or ""))
            table.setItem(row, 2, QTableWidgetItem(rec.location or ""))
            table.setItem(row, 3, QTableWidgetItem(rec.status or ""))
            table.setItem(row, 4, QTableWidgetItem(rec.comment or ""))
            table.setItem(row, 5, QTableWidgetItem(rec.registered_for or ""))
            table.setItem(row, 6, QTableWidgetItem(rec.registered_by or ""))
            table.setItem(row, 7,
                          QTableWidgetItem(rec.last_updated.strftime('%Y-%m-%d %H:%M') if rec.last_updated else ""))

        table.resizeColumnsToContents()
        layout.addWidget(table)

        btn_layout = QHBoxLayout()
        edit_btn = QPushButton("✏️ ویرایش رکورد")
        delete_btn = QPushButton("🗑️ حذف رکورد")
        edit_items_btn = QPushButton("✏️ ویرایش آیتم‌های مصرفی")
        close_btn = QPushButton("بستن")

        btn_layout.addWidget(edit_btn)
        btn_layout.addWidget(delete_btn)
        btn_layout.addWidget(edit_items_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(close_btn)
        layout.addLayout(btn_layout)

        def get_selected_record_id():
            selected = table.currentRow()
            if selected < 0: return None
            return int(table.item(selected, 0).text())

        def edit_record():
            record_id = get_selected_record_id()
            if not record_id:
                self.show_message("خطا", "لطفاً یک رکورد را انتخاب کنید.", "warning")
                return
            record = next((r for r in records if r.id == record_id), None)
            if not record: return
            new_location, ok1 = QInputDialog.getText(self, "ویرایش Location", "مقدار جدید:", text=record.location or "")
            if not ok1: return
            new_status, ok2 = QInputDialog.getText(self, "ویرایش Status", "مقدار جدید:", text=record.status or "")
            if not ok2: return
            success, msg = self.dm.update_miv_record(
                record_id, {"location": new_location, "status": new_status}, user=self.current_user)
            self.show_message("نتیجه", msg, "success" if success else "error")
            if success:
                dlg.close()
                self.update_line_dashboard()

        def delete_record():
            record_id = get_selected_record_id()
            if not record_id:
                self.show_message("خطا", "لطفاً یک رکورد را انتخاب کنید.", "warning")
                return
            confirm = QMessageBox.question(
                self, "تأیید حذف", f"آیا مطمئن هستید که رکورد {record_id} حذف شود؟ این عمل غیرقابل بازگشت است.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if confirm == QMessageBox.StandardButton.Yes:
                success, msg = self.dm.delete_miv_record(record_id)
                self.show_message("نتیجه", msg, "success" if success else "error")
                if success:
                    dlg.close()
                    self.update_line_dashboard()

        def edit_items():
            record_id = get_selected_record_id()
            if not record_id:
                self.show_message("خطا", "یک رکورد انتخاب نشده.", "warning")
                return
            record = next((r for r in records if r.id == record_id), None)
            if not record: return
            dialog = MTOConsumptionDialog(self.dm, record.project_id, record.line_no, miv_record_id=record_id,
                                          parent=self)
            if dialog.exec():
                consumed_items, spool_items = dialog.get_data()
                success, msg = self.dm.update_miv_items(record_id, consumed_items, spool_items, user=self.current_user)
                self.show_message("نتیجه", msg, "success" if success else "error")
                if success:
                    dlg.close()
                    self.update_line_dashboard()

        edit_btn.clicked.connect(edit_record)
        delete_btn.clicked.connect(delete_record)
        edit_items_btn.clicked.connect(edit_items)
        close_btn.clicked.connect(dlg.close)
        dlg.exec()

    def update_line_dashboard(self):
        if not self.current_project:
            return

        line_no = self.entries["Line No"].text().strip()
        self.dashboard_ax.clear()

        if not line_no:
            self.dashboard_ax.text(0.5, 0.5, "Please enter the line number", ha='center', va='center')
            self.canvas.draw()
            return

        progress = self.dm.get_line_progress(self.current_project.id, line_no)
        percentage = progress.get("percentage", 0)

        if progress["total_weight"] == 0:
            self.dashboard_ax.text(0.5, 0.5, "No data found for this line", ha='center', va='center')
            self.canvas.draw()
            return

        labels = ['Used', 'Remaining']

        sizes = [percentage, 100 - percentage]
        colors = ['#4CAF50', '#BDBDBD']
        explode = (0.1, 0) if percentage > 0 else (0, 0)

        self.dashboard_ax.pie(
            sizes, explode=explode, labels=labels, colors=colors,
            autopct='%1.1f%%', shadow=True, startangle=90
        )
        self.dashboard_ax.axis('equal')
        self.dashboard_ax.set_title(f"Line progress: {line_no} ({percentage}%)")

        self.fig.tight_layout()
        self.canvas.draw()

    def log_to_console(self, message, level="info"):
        color_map = {"info": "#8be9fd", "success": "#50fa7b", "warning": "#f1fa8c", "error": "#ff5555"}
        color = color_map.get(level, "#f8f8f2")
        formatted_message = f'<span style="color: {color};">{message}</span>'
        self.console_output.append(formatted_message)

    def show_message(self, title, message, level="info"):
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle(title)
        msg_box.setText(message)
        icon_map = {"info": QMessageBox.Icon.Information, "warning": QMessageBox.Icon.Warning,
                    "error": QMessageBox.Icon.Critical}
        msg_box.setIcon(icon_map.get(level, QMessageBox.Icon.NoIcon))
        msg_box.exec()

    def show_line_details(self):
        # 🔒 گرفتن رمز با ستاره
        dlg = QInputDialog(self)
        dlg.setWindowTitle("ورود رمز")
        dlg.setLabelText("رمز داشبورد را وارد کنید:")
        dlg.setTextEchoMode(QLineEdit.EchoMode.Password)  # ⭐ نمایش به صورت ستاره
        ok = dlg.exec()

        password = dlg.textValue()

        if not ok or password != self.dashboard_password:
            self.show_message("خطا", "رمز اشتباه است یا عملیات لغو شد.", "error")
            return

        # ✅ اگر رمز درست بود ادامه بده
        python_executable = sys.executable
        try:
            if not hasattr(self, 'api_process') or self.api_process.poll() is not None:
                self.api_process = subprocess.Popen([python_executable, "report_api.py"])

            if not hasattr(self, 'dashboard_process') or self.dashboard_process.poll() is not None:
                self.dashboard_process = subprocess.Popen([python_executable, "dashboard.py"])

            webbrowser.open("http://127.0.0.1:8050")

        except Exception as e:
            self.show_message("خطا", f"خطا در اجرای سرورهای گزارش‌گیری: {e}", "error")

    def cleanup_processes(self):
        """کشتن کامل پروسه‌های جانبی"""
        try:
            if hasattr(self, 'api_process') and self.api_process:
                self.api_process.kill()
            if hasattr(self, 'dashboard_process') and self.dashboard_process:
                self.dashboard_process.kill()
        except Exception as e:
            print(f"⚠️ خطا در بستن پروسه‌ها: {e}")

    def open_spool_manager(self):
        dlg = QInputDialog(self)
        dlg.setWindowTitle("ورود رمز")
        dlg.setLabelText("رمز را وارد کنید:")
        dlg.setTextEchoMode(QLineEdit.EchoMode.Password)  # ⭐ نمایش به صورت ستاره
        ok = dlg.exec()

        password = dlg.textValue()

        if not ok or password != self.dashboard_password:
            self.show_message("خطا", "رمز اشتباه است یا عملیات لغو شد.", "error")
            return

        # ✅ اگر رمز درست بود ادامه بده
        python_executable = sys.executable
        dialog = SpoolManagerDialog(self.dm, self)
        dialog.exec()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()


    def excepthook(exc_type, exc_value, exc_tb):
        error_msg = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        print("Unhandled exception:", error_msg)

        box = QMessageBox()
        box.setIcon(QMessageBox.Icon.Critical)
        box.setWindowTitle("Unhandled Exception")
        box.setText("خطای غیرمنتظره رخ داد")
        box.setDetailedText(error_msg)  # ✨ متن کامل استک‌ترِیس
        box.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse |
            Qt.TextInteractionFlag.TextSelectableByKeyboard
        )
        box.exec()


    sys.excepthook = excepthook
    sys.exit(app.exec())