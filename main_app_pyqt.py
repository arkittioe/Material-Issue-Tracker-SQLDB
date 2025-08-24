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
    QCompleter, QInputDialog, QFileDialog, QGroupBox, QProgressBar
)

from PyQt6.QtGui import QFont, QColor
from PyQt6.QtCore import Qt, QStringListModel, pyqtSignal, QObject

# برای نمایش نمودار در PyQt6
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

# فرض بر این است که این دو فایل در کنار این اسکریپت قرار دارند
from data_manager import DataManager
from models import Project, MTOItem, MIVRecord, Spool, SpoolItem  # برای type hinting

import sys, traceback

import threading
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from config_manager import DB_PATH, DASHBOARD_PASSWORD, ISO_PATH

class IsoIndexEventHandler(QObject, FileSystemEventHandler):  # 👈 **ORDER SWAPPED HERE**
    """
    This class reacts to file system changes (create, delete, modify)
    and calls the appropriate DataManager functions to update the database.
    """
    status_updated = pyqtSignal(str, str)
    progress_updated = pyqtSignal(int)

    def __init__(self, dm: DataManager):
        # The super().__init__() call now correctly initializes the QObject first.
        super().__init__()
        # We no longer need to call FileSystemEventHandler.__init__() separately.

        self.dm = dm
        self.SUPPORTED_EXTENSIONS = {".pdf", ".dwg"}

    def _is_supported(self, path):
        return os.path.splitext(path)[1].lower() in self.SUPPORTED_EXTENSIONS

    def on_created(self, event):
        if not event.is_directory and self._is_supported(event.src_path):
            print(f"File created: {event.src_path}")
            self.dm.upsert_iso_index_entry(event.src_path)

    def on_deleted(self, event):
        if not event.is_directory and self._is_supported(event.src_path):
            print(f"File deleted: {event.src_path}")
            self.dm.remove_iso_index_entry(event.src_path)

    def on_modified(self, event):
        if not event.is_directory and self._is_supported(event.src_path):
            print(f"File modified: {event.src_path}")
            self.dm.upsert_iso_index_entry(event.src_path)

    def on_moved(self, event):
        if not event.is_directory and self._is_supported(event.src_path):
            print(f"File moved: from {event.src_path} to {event.dest_path}")
            self.dm.remove_iso_index_entry(event.src_path)
            if self._is_supported(event.dest_path):
                self.dm.upsert_iso_index_entry(event.dest_path)


class SpoolManagerDialog(QDialog):
    def __init__(self, dm: DataManager, parent=None):
        super().__init__(parent)
        self.dm = dm
        self.setWindowTitle("مدیریت اسپول‌ها")
        self.setMinimumSize(1200, 700)  # کمی عرض را بیشتر کردم
        self.current_spool_id = None
        self.is_new_spool = True

        layout = QVBoxLayout(self)
        top_groupbox = QGroupBox("اطلاعات اسپول")
        top_layout = QHBoxLayout()
        form_layout = QFormLayout()

        self.spool_id_entry = QLineEdit()
        self.spool_id_entry.setPlaceholderText("شناسه اسپول را وارد یا انتخاب کنید...")
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

        self.setup_spool_id_completer()

        self.table = QTableWidget()
        # --- CHANGE: اضافه شدن ستون Thickness ---
        self.table.setColumnCount(10)
        self.table.setHorizontalHeaderLabels([
            "Component Type", "Class/Angle", "Bore1", "Bore2",
            "Material", "Schedule", "Thickness", "Length (m)", "Qty Available", "Item Code"
        ])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.table)

        # ... (بخش دکمه‌ها بدون تغییر) ...
        btns_layout = QHBoxLayout()
        self.add_row_btn = QPushButton("➕ افزودن ردیف")
        self.remove_row_btn = QPushButton("➖ حذف ردیف")
        self.export_btn = QPushButton("خروجی اکسل")
        self.save_btn = QPushButton("💾 ذخیره تغییرات")
        self.close_btn = QPushButton("بستن")
        btns_layout.addWidget(self.add_row_btn)
        btns_layout.addWidget(self.remove_row_btn)
        btns_layout.addStretch()
        btns_layout.addWidget(self.export_btn)
        btns_layout.addWidget(self.save_btn)
        btns_layout.addWidget(self.close_btn)
        layout.addLayout(btns_layout)

        self.load_btn.clicked.connect(self.load_spool)
        self.new_btn.clicked.connect(self.new_spool)
        self.add_row_btn.clicked.connect(self.add_row)
        self.remove_row_btn.clicked.connect(self.remove_row)
        self.save_btn.clicked.connect(self.save_changes)
        self.export_btn.clicked.connect(self.handle_export_to_excel)
        self.close_btn.clicked.connect(self.close)

    def setup_spool_id_completer(self):
        """لیست شناسه‌های اسپول را از دیتابیس گرفته و به ورودی اضافه می‌کند."""
        try:
            spool_ids = self.dm.get_all_spool_ids()
            model = QStringListModel()
            model.setStringList(spool_ids)
            completer = QCompleter(model, self)
            completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
            self.spool_id_entry.setCompleter(completer)
        except Exception as e:
            print(f"Failed to setup completer: {e}")

    def populate_table(self, items: list[SpoolItem]):
        """جدول را با آیتم‌های یک اسپول پر می‌کند."""
        self.table.setRowCount(len(items))
        for row, item in enumerate(items):
            def to_str(val):
                return str(val) if val is not None else ""

            self.table.setItem(row, 0, QTableWidgetItem(item.component_type or ""))
            self.table.setItem(row, 1, QTableWidgetItem(item.class_angle or ""))
            self.table.setItem(row, 2, QTableWidgetItem(to_str(item.p1_bore)))
            self.table.setItem(row, 3, QTableWidgetItem(to_str(item.p2_bore)))
            self.table.setItem(row, 4, QTableWidgetItem(item.material or ""))
            self.table.setItem(row, 5, QTableWidgetItem(item.schedule or ""))
            # --- CHANGE: نمایش مقدار Thickness در ستون جدید ---
            self.table.setItem(row, 6, QTableWidgetItem(to_str(item.thickness)))
            self.table.setItem(row, 7, QTableWidgetItem(to_str(item.length)))
            self.table.setItem(row, 8, QTableWidgetItem(to_str(item.qty_available)))
            self.table.setItem(row, 9, QTableWidgetItem(item.item_code or ""))

    def add_row(self):
        self.table.insertRow(self.table.rowCount())

    def remove_row(self):
        row = self.table.currentRow()
        if row >= 0:
            self.table.removeRow(row)

    def load_spool(self):
        """یک اسپول موجود را برای ویرایش بارگذاری می‌کند."""
        # --- CHANGE: تبدیل شناسه به حروف بزرگ ---
        spool_id = self.spool_id_entry.text().strip().upper()
        if not spool_id:
            self.show_msg("هشدار", "لطفاً شناسه اسپول را برای بارگذاری وارد کنید.", icon=QMessageBox.Icon.Warning)
            return

        spool = self.dm.get_spool_by_id(spool_id)
        if not spool:
            self.show_msg("خطا", f"اسپولی با شناسه '{spool_id}' یافت نشد.", icon=QMessageBox.Icon.Critical)
            return

        self.current_spool_id = spool.spool_id
        self.spool_id_entry.setText(spool.spool_id)
        self.location_entry.setText(spool.location or "")
        self.populate_table(spool.items)
        self.is_new_spool = False
        self.log_to_console(f"اسپول '{spool_id}' برای ویرایش بارگذاری شد.", "success")

    def new_spool(self):
        """فرم را برای ایجاد یک اسپول جدید آماده می‌کند."""
        self.current_spool_id = None
        next_id = self.dm.generate_next_spool_id()
        self.spool_id_entry.setText(next_id)
        self.location_entry.clear()
        self.table.setRowCount(0)
        self.is_new_spool = True
        self.log_to_console(f"فرم برای ثبت اسپول جدید ({next_id}) آماده است.", "info")

    def save_changes(self):
        """تغییرات جدول و اطلاعات را در دیتابیس ذخیره می‌کند."""
        # --- CHANGE: تبدیل شناسه به حروف بزرگ ---
        spool_id = self.spool_id_entry.text().strip().upper()
        if not spool_id:
            self.show_msg("هشدار", "Spool ID الزامی است.", icon=QMessageBox.Icon.Warning)
            return

        try:
            def safe_float(txt):
                if txt is None: return None
                s = str(txt).strip()
                if not s: return None
                try:
                    return round(float(s), 2)
                except (ValueError, TypeError):
                    return None

            items_data = []
            for r in range(self.table.rowCount()):
                def get_item_text(row, col, to_upper=False):
                    item = self.table.item(row, col)
                    text = item.text().strip() if item and item.text() else None
                    # --- CHANGE: تبدیل فیلدهای متنی به حروف بزرگ ---
                    if text and to_upper:
                        return text.upper()
                    return text

                row_data = {
                    "component_type": get_item_text(r, 0, to_upper=True),
                    "class_angle": get_item_text(r, 1, to_upper=True),
                    "p1_bore": safe_float(get_item_text(r, 2)),
                    "p2_bore": safe_float(get_item_text(r, 3)),
                    "material": get_item_text(r, 4, to_upper=True),
                    "schedule": get_item_text(r, 5, to_upper=True),
                    # --- CHANGE: خواندن مقدار Thickness از ستون جدید ---
                    "thickness": safe_float(get_item_text(r, 6)),
                    "length": safe_float(get_item_text(r, 7)),
                    "qty_available": safe_float(get_item_text(r, 8)),
                    "item_code": get_item_text(r, 9, to_upper=True)
                }
                if row_data["component_type"]:
                    items_data.append(row_data)

            spool_data = {
                "spool_id": spool_id,
                "location": self.location_entry.text().strip() or None
            }

            if self.is_new_spool:
                success, msg = self.dm.create_spool(spool_data, items_data)
                if success:
                    self.is_new_spool = False
                    self.current_spool_id = spool_id
            else:
                if self.current_spool_id != spool_id:
                    reply = QMessageBox.question(self, 'تغییر شناسه اسپول',
                                                 f"شناسه اسپول از '{self.current_spool_id}' به '{spool_id}' تغییر کرده. آیا یک اسپول جدید با این شناسه ساخته شود؟",
                                                 QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
                    if reply == QMessageBox.StandardButton.Yes:
                        success, msg = self.dm.create_spool(spool_data, items_data)
                    else:
                        return
                else:
                    success, msg = self.dm.update_spool(self.current_spool_id, spool_data, items_data)

            if success:
                self.show_msg("موفق", msg)
                self.setup_spool_id_completer()
            else:
                self.show_msg("خطا", msg, icon=QMessageBox.Icon.Critical)

        except Exception as e:
            import traceback
            self.show_msg("خطای بحرانی", "عملیات ذخیره‌سازی ناموفق بود.", detailed=traceback.format_exc(),
                          icon=QMessageBox.Icon.Critical)

    # ... (بقیه توابع کلاس بدون تغییر باقی می‌مانند) ...
    def handle_export_to_excel(self):
        try:
            path, _ = QFileDialog.getSaveFileName(self, "ذخیره فایل اکسل", "Spool_Data.xlsx", "Excel Files (*.xlsx)")
            if not path:
                return
            ok, message = self.dm.export_spool_data_to_excel(path)
            icon = QMessageBox.Icon.Information if ok else QMessageBox.Icon.Critical
            self.show_msg("خروجی اکسل", message, icon=icon)
        except Exception as e:
            self.show_msg("خطا", "Export به اکسل با خطا مواجه شد.", detailed=str(e), icon=QMessageBox.Icon.Critical)

    def show_msg(self, title, text, detailed=None, icon=QMessageBox.Icon.Information):
        box = QMessageBox(self)
        box.setIcon(icon)
        box.setWindowTitle(title)
        box.setText(text)
        if detailed:
            box.setDetailedText(detailed)
        box.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse | Qt.TextInteractionFlag.TextSelectableByKeyboard)
        box.exec()

    def log_to_console(self, message, level="info"):
        if hasattr(self.parent(), 'log_to_console'):
            self.parent().log_to_console(message, level)
        else:
            print(f"[{level.upper()}] {message}")


class SpoolSelectionDialog(QDialog):
    def __init__(self, matching_items: list[SpoolItem], remaining_mto_qty: float, parent=None):
        super().__init__(parent)
        self.setWindowTitle("انتخاب آیتم از انبار اسپول")
        self.setMinimumSize(1200, 700)

        self.selected_data = []
        self.items = matching_items
        self.remaining_mto_qty = remaining_mto_qty

        layout = QVBoxLayout(self)

        # ... (بخش فیلتر بدون تغییر باقی می‌ماند) ...
        filter_group = QGroupBox("فیلتر")
        filter_layout = QGridLayout(filter_group)
        self.filters = {}
        filter_definitions = {"Item Code": 2, "Comp. Type": 3, "Material": 7, "Bore1": 5}
        col = 0
        for label, col_idx in filter_definitions.items():
            filter_label = QLabel(f"{label}:")
            filter_input = QLineEdit()
            filter_input.setPlaceholderText(f"جستجو بر اساس {label}...")
            filter_input.textChanged.connect(self.filter_table)
            filter_layout.addWidget(filter_label, 0, col)
            filter_layout.addWidget(filter_input, 0, col + 1)
            self.filters[col_idx] = filter_input
            col += 2
        layout.addWidget(filter_group)

        # --- بخش اطلاعات با لیبل جدید ---
        info_layout = QHBoxLayout()
        info_label = QLabel(f"مقدار کل باقی‌مانده از MTO: {self.remaining_mto_qty}")
        info_label.setStyleSheet("background-color: #f1fa8c; padding: 5px; border-radius: 3px;")

        # <<< NEW: لیبل برای نمایش جمع کل انتخاب شده
        self.total_selected_label = QLabel("جمع انتخاب شده: 0.0")
        self.total_selected_label.setStyleSheet("font-weight: bold; padding: 5px; background-color: #d1e7dd;")

        info_layout.addWidget(info_label, 1)
        info_layout.addWidget(self.total_selected_label)
        layout.addLayout(info_layout)

        # ... (بخش جدول و دکمه‌ها بدون تغییر باقی می‌ماند) ...
        self.table = QTableWidget()
        self.table.setColumnCount(14)
        self.table.setHorizontalHeaderLabels([
            "ID", "Spool ID", "Item Code", "Comp. Type", "Class/Angle", "Bore1", "Bore2",
            "Material", "Schedule", "Thickness", "Length", "Qty Avail.", "موجودی", "مقدار مصرف"
        ])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        layout.addWidget(self.table)

        self.populate_table()

        self.buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.buttons.accepted.connect(self.accept_data)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def populate_table(self):
        self.spin_boxes_info = []
        self.table.setRowCount(len(self.items))

        for row, item in enumerate(self.items):
            self.table.setItem(row, 0, QTableWidgetItem(str(item.id)))
            self.table.setItem(row, 1, QTableWidgetItem(str(item.spool.spool_id)))
            # ... (ستون‌های 2 تا 9 بدون تغییر)
            self.table.setItem(row, 2, QTableWidgetItem(item.item_code or ""))
            self.table.setItem(row, 3, QTableWidgetItem(item.component_type or ""))
            self.table.setItem(row, 4, QTableWidgetItem(str(item.class_angle) if item.class_angle is not None else ""))
            self.table.setItem(row, 5, QTableWidgetItem(str(item.p1_bore or "")))
            self.table.setItem(row, 6, QTableWidgetItem(str(item.p2_bore or "")))
            self.table.setItem(row, 7, QTableWidgetItem(item.material or ""))
            self.table.setItem(row, 8, QTableWidgetItem(item.schedule or ""))
            self.table.setItem(row, 9, QTableWidgetItem(str(item.thickness or "")))

            self.table.setItem(row, 10, QTableWidgetItem(str(item.length or "")))
            self.table.setItem(row, 11, QTableWidgetItem(str(item.qty_available or "")))

            # --- CHANGE: حذف تبدیل واحد ---
            is_pipe = "PIPE" in (item.component_type or "").upper()
            if is_pipe:
                available_qty_for_ui = item.length or 0  # دیگر تقسیم بر ۱۰۰۰ نداریم
            else:
                available_qty_for_ui = item.qty_available or 0

            # نمایش موجودی با دو رقم اعشار
            self.table.setItem(row, 12, QTableWidgetItem(f"{available_qty_for_ui:.2f}"))

            spin_box = QDoubleSpinBox()
            spin_box.setRange(0, available_qty_for_ui)
            # --- CHANGE: تنظیم دقت به ۲ رقم اعشار ---
            spin_box.setDecimals(2)
            spin_box.valueChanged.connect(self.update_totals)
            self.table.setCellWidget(row, 13, spin_box)

            self.spin_boxes_info.append({'widget': spin_box, 'max_avail': available_qty_for_ui})

            for col in range(13):
                cell_item = self.table.item(row, col)
                if cell_item:
                    cell_item.setFlags(cell_item.flags() & ~Qt.ItemFlag.ItemIsEditable)

        self.update_totals()

    def accept_data(self):
        self.selected_data = []
        for row in range(self.table.rowCount()):
            if self.table.isRowHidden(row):
                continue

            spin_box = self.table.cellWidget(row, 13)
            used_qty_from_ui = spin_box.value()

            if used_qty_from_ui > 0.001:
                spool_item_id = int(self.table.item(row, 0).text())

                # --- CHANGE: حذف تبدیل واحد و گرد کردن نهایی ---
                used_qty_for_db = round(used_qty_from_ui, 2)

                self.selected_data.append({
                    "spool_item_id": spool_item_id,
                    "used_qty": used_qty_for_db
                })
        self.accept()

    def get_selected_data(self):
        return self.selected_data

    def filter_table(self):
        """Hides rows that do not match the filter criteria."""
        # --- CHANGE: تبدیل به حروف بزرگ برای جستجوی غیرحساس به بزرگی و کوچکی ---
        filter_texts = {col: f.text().upper() for col, f in self.filters.items()}

        for row in range(self.table.rowCount()):
            is_visible = True
            for col, filter_text in filter_texts.items():
                if not filter_text:
                    continue
                item = self.table.item(row, col)
                # --- CHANGE: متن سلول هم به حروف بزرگ تبدیل می‌شود ---
                if not item or filter_text not in item.text().upper():
                    is_visible = False
                    break
            self.table.setRowHidden(row, not is_visible)

    def update_totals(self):
        """Calculates the total selected quantity and dynamically updates the limits of all spin boxes."""
        current_total = sum(info['widget'].value() for info in self.spin_boxes_info)

        # --- CHANGE: آپدیت لیبل با دو رقم اعشار ---
        self.total_selected_label.setText(f"جمع انتخاب شده: {current_total:.2f}")
        if current_total > self.remaining_mto_qty:
            self.total_selected_label.setStyleSheet("font-weight: bold; padding: 5px; background-color: #f8d7da;")
        else:
            self.total_selected_label.setStyleSheet("font-weight: bold; padding: 5px; background-color: #d1e7dd;")

        remaining_headroom = self.remaining_mto_qty - current_total

        for info in self.spin_boxes_info:
            spin_box = info['widget']
            new_max = min(info['max_avail'], spin_box.value() + remaining_headroom)

            spin_box.blockSignals(True)
            spin_box.setMaximum(max(0, new_max))
            spin_box.blockSignals(False)


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

            # ستون‌های MTO (0-7)
            self.table.setItem(row_idx, 0, QTableWidgetItem(item["Item Code"] or ""))
            self.table.setItem(row_idx, 1, QTableWidgetItem(item["Description"] or ""))
            self.table.setItem(row_idx, 2, QTableWidgetItem(str(item["Total Qty"])))
            self.table.setItem(row_idx, 3, QTableWidgetItem(str(item["Used Qty"])))
            remaining_qty = item["Remaining Qty"] or 0
            self.table.setItem(row_idx, 4, QTableWidgetItem(str(remaining_qty)))
            self.table.setItem(row_idx, 5, QTableWidgetItem(item["Unit"] or ""))
            self.table.setItem(row_idx, 6, QTableWidgetItem(str(item.get("Bore") or "")))
            self.table.setItem(row_idx, 7, QTableWidgetItem(item.get("Type") or ""))

            # مصرف موجود در این MIV
            current_miv_total_usage = self.existing_consumptions.get(mto_item_id, 0)

            # SpinBox برای مصرف مستقیم
            spin_box = QDoubleSpinBox()
            max_val = remaining_qty + current_miv_total_usage
            spin_box.setRange(0, max_val)
            spin_box.setDecimals(2)
            spin_box.setValue(current_miv_total_usage)
            self.table.setCellWidget(row_idx, 8, spin_box)

            # دکمه انتخاب اسپول
            spool_btn = QPushButton("انتخاب...")

            # --- NEW: بررسی سازگاری آیتم با انبار اسپول ---
            item_type = item.get("Type")
            p1_bore = item.get("Bore")
            # از تابع جدید DataManager برای گرفتن آیتم‌های سازگار استفاده می‌کنیم
            matching_items = self.dm.get_mapped_spool_items(item_type, p1_bore)

            if not matching_items:  # 🚫 اگر هیچ اسپولی پیدا نشد
                spool_btn.setEnabled(False)
                spool_btn.setToolTip("هیچ آیتم سازگاری در انبار اسپول یافت نشد.")

            spool_btn.clicked.connect(partial(self.handle_spool_selection, row_idx))
            self.table.setCellWidget(row_idx, 9, spool_btn)

            # ستون‌های اطلاعات اسپول
            for col in [10, 11, 12]:
                self.table.setItem(row_idx, col, QTableWidgetItem(""))

            # اگر کلا آیتمی باقی نمانده، همه کنترل‌ها غیرفعال شوند
            if max_val <= 0:
                spin_box.setEnabled(False)
                spool_btn.setEnabled(False)

            # ستون‌های اطلاعاتی فقط-خواندنی
            for col in list(range(8)) + [10, 11, 12]:
                item_widget = self.table.item(row_idx, col)
                if item_widget:
                    item_widget.setFlags(item_widget.flags() & ~Qt.ItemFlag.ItemIsEditable)

        self.table.resizeColumnsToContents()

    def handle_spool_selection(self, row_idx):
        item_data = self.progress_data[row_idx]
        item_type = item_data.get("Type")
        p1_bore = item_data.get("Bore")

        # --- NEW: Get the remaining quantity for the MTO item ---
        remaining_qty = item_data.get("Remaining Qty", 0)

        if not item_type:
            self.parent().show_message("هشدار", "نوع آیتم (Type) برای این ردیف MTO مشخص نشده است.", "warning")
            return

        # 🔹 استفاده درست از item_data به جای item
        matching_items = self.dm.get_mapped_spool_items(item_type, p1_bore)

        if not matching_items:
            self.parent().show_message(
                "اطلاعات",
                f"هیچ اسپول سازگار برای نوع '{item_type}' و سایز '{p1_bore}' یافت نشد.",
                "info"
            )
            return

        # --- CHANGE: Pass the remaining_qty to the dialog ---
        dialog = SpoolSelectionDialog(matching_items, remaining_qty, self)
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
            spool_item = session.get(SpoolItem, first_selection['spool_item_id'])
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

        for row in range(self.table.rowCount()):
            mto_item_id = self.progress_data[row]["mto_item_id"]

            # مصرف مستقیم
            spin_box = self.table.cellWidget(row, 8)
            direct_qty = spin_box.value() if spin_box else 0
            if direct_qty > 0.001:
                self.consumed_data.append({
                    "mto_item_id": mto_item_id,
                    # --- CHANGE: گرد کردن مقدار نهایی ---
                    "used_qty": round(direct_qty, 2)
                })

            # مصرف اسپول (مقادیر از دیالوگ دیگر گرد شده می‌آیند)
            if row in self.spool_selections:
                for sel in self.spool_selections[row]:
                    self.spool_consumption_data.append({
                        "spool_item_id": sel["spool_item_id"],
                        "used_qty": sel["used_qty"] # این مقدار از قبل گرد شده
                    })

        self.accept()

    def get_data(self):
        return self.consumed_data, self.spool_consumption_data

# --- پنجره اصلی برنامه ---
class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("مدیریت MIV - نسخه 1.0")
        self.setGeometry(100, 100, 1200, 800)

        self.dm = DataManager(db_path=DB_PATH)
        self.current_project: Project | None = None
        self.current_user = os.getlogin()
        self.suggestion_data = []
        self.dashboard_password = "hossein"#DASHBOARD_PASSWORD

        self.iso_observer = None  # متغیر برای نگه داشتن ترد نگهبان

        # تعریف یک سیگنال در کلاس اصلی برای دریافت پیام از ترد نگهبان
        self.iso_event_handler = IsoIndexEventHandler(self.dm)
        # --- NEW: راه‌اندازی منوی بالای پنجره ---
        self.setup_menu()
        self.setup_ui()
        self.connect_signals()
        self.populate_project_combo()
        QApplication.instance().aboutToQuit.connect(self.cleanup_processes)

        self.start_iso_watcher()

    def setup_menu(self):
        """یک منوی Help در بالای پنجره اصلی ایجاد می‌کند."""
        # ساخت منو بار
        menu_bar = self.menuBar()
        # اضافه کردن منوی Help (راهنما)
        help_menu = menu_bar.addMenu("&Help")
        # اضافه کردن گزینه About (درباره ما) به منوی Help
        about_action = help_menu.addAction("&About")
        # اتصال کلیک روی گزینه About به تابع نمایش دیالوگ
        about_action.triggered.connect(self.show_about_dialog)

    def setup_ui(self):
        """متد اصلی برای ساخت و چیدمان تمام ویجت‌ها."""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        # --- CHANGE: چیدمان اصلی به QVBoxLayout تغییر کرد تا بتوانیم لیبل را در پایین اضافه کنیم ---
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(10, 10, 10, 5) # تنظیم فاصله از لبه‌ها

        splitter = QSplitter(Qt.Orientation.Horizontal)

        left_panel = QFrame()
        left_layout = QVBoxLayout(left_panel)
        reg_form_frame = QFrame()
        reg_form_frame.setFrameShape(QFrame.Shape.StyledPanel)
        self.create_registration_form(reg_form_frame)
        dashboard_frame = QFrame()
        dashboard_frame.setFrameShape(QFrame.Shape.StyledPanel)
        self.create_dashboard(dashboard_frame)
        left_layout.addWidget(reg_form_frame)
        left_layout.addWidget(dashboard_frame, 1)

        right_panel = QFrame()
        right_layout = QVBoxLayout(right_panel)
        search_frame = QFrame()
        search_frame.setFrameShape(QFrame.Shape.StyledPanel)
        self.create_search_box(search_frame)
        console_frame = QFrame()
        console_frame.setFrameShape(QFrame.Shape.StyledPanel)
        self.create_console(console_frame)
        right_layout.addWidget(search_frame)
        right_layout.addWidget(console_frame, 1)

        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setSizes([550, 650])

        # اسپلیتر به چیدمان اصلی اضافه می‌شود
        main_layout.addWidget(splitter)

        # --- NEW: اضافه کردن لیبل نام سازنده در پایین پنجره ---
        dev_label = QLabel("Developed by h.izadi")
        # استایل برای کم‌رنگ کردن و راست‌چین کردن متن
        dev_label.setStyleSheet("color: #777; padding-top: 4px;")
        dev_label.setAlignment(Qt.AlignmentFlag.AlignRight)
        main_layout.addWidget(dev_label)

    def create_registration_form(self, parent_widget):

        # # ساخت لایه‌ی اصلی فرم ثبت
        layout = QVBoxLayout(parent_widget)  # # چیدمان عمودی برای فرم
        layout.addWidget(QLabel("<h2>ثبت رکورد MIV جدید</h2>"))  # # عنوان فرم


        form_layout = QFormLayout()  # # فرم دوبخشی لیبل/فیلد
        self.entries = {}  # # دیکشنری نگهداری ویجت‌های ورودی
        # # --- ردیف ویژه برای Line No با دکمه جستجوی فایل ---
        line_row_container = QWidget()  # # کانتینر برای چینش افقی Line No + دکمه
        line_row = QHBoxLayout(line_row_container)  # # چیدمان افقی
        line_row.setContentsMargins(0, 0, 0, 0)  # # بدون حاشیه

        self.entries["Line No"] = QLineEdit()  # # ورودی شماره خط
        self.entries["Line No"].setPlaceholderText(
            "شماره خط را وارد کنید (مثال: 10\"-P-210415-D6D-P).")  # # راهنمای ورودی

        self.iso_search_btn = QPushButton("🔎 جستجوی فایل‌های ISO/DWG")  # # دکمه جدید برای جستجو
        self.iso_search_btn.setToolTip(
            "جستجو در Y:\\Piping\\ISO بر اساس 6 رقم اولِ Line No (بدون توجه به علائم و حروف).")  # # توضیح

        line_row.addWidget(self.entries["Line No"], 1)  # # افزودن ورودی به ردیف
        line_row.addWidget(self.iso_search_btn)  # # افزودن دکمه جستجو

        form_layout.addRow("Line No:", line_row_container)  # # اضافه کردن ردیف Line No به فرم

        # # --- بقیه فیلدها مثل قبل ---
        for field in ["MIV Tag", "Location", "Status", "Registered For"]:  # # لیست فیلدهای دیگر
            self.entries[field] = QLineEdit()  # # ایجاد ورودی
            form_layout.addRow(f"{field}:", self.entries[field])  # # افزودن به فرم

        # # راه‌اندازی Completer برای Line No (مثل قبل)
        self.line_completer_model = QStringListModel()  # # مدل کامپلتر
        self.line_completer = QCompleter(self.line_completer_model, self)  # # خود کامپلتر
        self.line_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)  # # حساس نبودن به بزرگی حروف
        self.line_completer.setFilterMode(Qt.MatchFlag.MatchContains)  # # جستجوی شامل
        self.entries["Line No"].setCompleter(self.line_completer)  # # اتصال کامپلتر به فیلد

        # # اتصال دکمه جستجو به هندلر جدید
        self.iso_search_btn.clicked.connect(self.handle_iso_search)  # # اتصال کلیک به تابع جستجو و نمایش نتایج

        self.register_btn = QPushButton("ثبت رکورد")  # # دکمه ثبت
        layout.addLayout(form_layout)  # # افزودن فرم به چیدمان
        layout.addWidget(self.register_btn)  # # افزودن دکمه ثبت
        layout.addStretch()  # # کشسان برای پر کردن فضا

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

        layout.addLayout(project_layout)

        # --- NEW: لیبل برای نمایش وضعیت همگام‌سازی ISO ---
        self.iso_status_label = QLabel("وضعیت ایندکس ISO: در حال بررسی...")
        self.iso_status_label.setStyleSheet("padding: 4px; color: #f1fa8c;")  # رنگ زرد برای حالت اولیه

        # --- NEW: دکمه‌های مدیریت و آپدیت داده ---
        management_layout = QHBoxLayout()
        self.manage_spool_btn = QPushButton("مدیریت اسپول‌ها")
        self.update_data_btn = QPushButton("🔄 به‌روزرسانی از CSV")  # دکمه جدید
        self.update_data_btn.setStyleSheet("background-color: #6272a4;")  # رنگ متمایز



        # --- NEW: اضافه کردن QProgressBar برای نمایش وضعیت ایندکس ---
        self.iso_progress_bar = QProgressBar()
        self.iso_progress_bar.setRange(0, 100)
        self.iso_progress_bar.setValue(0)
        self.iso_progress_bar.setTextVisible(True)
        self.iso_progress_bar.setFormat("ایندکس ISO: %p%")
        self.iso_progress_bar.hide()  # در ابتدا مخفی است


        self.console_output = QTextEdit()
        self.console_output.setReadOnly(True)
        self.console_output.setFont(QFont("Consolas", 11))
        self.console_output.setStyleSheet("background-color: #2b2b2b; color: #f8f8f2;")

        layout.addWidget(self.console_output, 1)
        layout.addLayout(management_layout)  # اضافه کردن چیدمان دکمه‌ها
        management_layout.addWidget(self.manage_spool_btn)
        management_layout.addWidget(self.update_data_btn)

        layout.addWidget(self.iso_progress_bar)

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

        self.update_data_btn.clicked.connect(self.handle_data_update_from_csv)
        self.iso_event_handler.status_updated.connect(self.update_iso_status_label)

        # --- NEW: اتصال سیگنال پیشرفت به اسلات جدید ---
        self.iso_event_handler.progress_updated.connect(self.update_iso_progress)

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

        form_data = {field: widget.text().strip().upper() for field, widget in self.entries.items()}
        form_data["Registered By"] = self.current_user
        form_data["Complete"] = False  # پیش‌فرض

        if not form_data["Line No"] or not form_data["MIV Tag"]:
            self.show_message("خطا", "فیلدهای Line No و MIV Tag اجباری هستند.", "warning")
            return

        if self.dm.is_duplicate_miv_tag(form_data["MIV Tag"], self.current_project.id):
            self.show_message("خطا", f"تگ '{form_data['MIV Tag']}' در این پروژه تکراری است.", "error")
            return

        # اطمینان از وجود رکوردهای پیشرفت برای این خط
        self.dm.initialize_mto_progress_for_line(self.current_project.id, form_data["Line No"])

        dialog = MTOConsumptionDialog(self.dm, self.current_project.id, form_data["Line No"], parent=self)
        if not dialog.exec():
            self.log_to_console("ثبت رکورد لغو شد.", "warning")
            return

        consumed_items, spool_items = dialog.get_data()
        if not consumed_items and not spool_items:
            self.log_to_console("ثبت رکورد لغو شد چون هیچ آیتمی مصرف نشده بود.", "warning")
            return

        # (بهینه‌سازی شده) ساخت کامنت بدون کوئری اضافه
        comment_parts = []
        if consumed_items:
            # dialog.progress_data حاوی تمام اطلاعات مورد نیاز است
            mto_info_map = {item['mto_item_id']: item for item in dialog.progress_data}
            for item in consumed_items:
                mto_details = mto_info_map.get(item['mto_item_id'])
                if mto_details:
                    identifier = mto_details.get("Item Code") or mto_details.get(
                        "Description") or f"ID {mto_details['mto_item_id']}"
                    comment_parts.append(f"{item['used_qty']} x {identifier}")

        form_data["Comment"] = " | ".join(comment_parts)

        success, msg = self.dm.register_miv_record(self.current_project.id, form_data, consumed_items, spool_items)

        if success:
            self.log_to_console(msg, "success")
            self.update_line_dashboard()
            # پاک کردن فیلدهای فرم پس از ثبت موفق
            for field in ["MIV Tag", "Location", "Status"]:
                if field in self.entries:
                    self.entries[field].clear()
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

    def handle_data_update_from_csv(self):
        """
        --- CHANGE: بازنویسی کامل تابع برای انتخاب چند فایل و پردازش هوشمند ---
        با انتخاب چند فایل CSV، آن‌ها را برای به‌روزرسانی پردازش می‌کند.
        """
        # ۱. گرفتن رمز برای عملیات حساس (بدون تغییر)
        dlg = QInputDialog(self)
        dlg.setWindowTitle("ورود رمز")
        dlg.setLabelText("این یک عملیات حساس است. لطفاً رمز را وارد کنید:")
        dlg.setTextEchoMode(QLineEdit.EchoMode.Password)
        if not dlg.exec() or dlg.textValue() != self.dashboard_password:
            self.show_message("خطا", "رمز اشتباه است یا عملیات لغو شد.", "error")
            return

        # ۲. نمایش هشدار کلی (بدون تغییر)
        confirm = QMessageBox.warning(self, "تایید عملیات بسیار مهم",
                                      "<b>هشدار!</b>\n\n"
                                      "شما در حال به‌روزرسانی داده‌ها از فایل‌های CSV هستید.\n"
                                      "این عملیات داده‌های موجود در دیتابیس را بر اساس فایل‌های انتخابی <b>جایگزین</b> خواهد کرد.\n\n"
                                      "<b>این عملیات غیرقابل بازگشت است. آیا مطمئن هستید؟</b>",
                                      QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
                                      QMessageBox.StandardButton.Cancel)
        if confirm == QMessageBox.StandardButton.Cancel:
            self.log_to_console("عملیات به‌روزرسانی داده لغو شد.", "warning")
            return

        # --- CHANGE: باز کردن دیالوگ انتخاب چند فایل به جای یک فولدر ---
        file_paths, _ = QFileDialog.getOpenFileNames(
            self,
            "فایل‌های CSV مورد نظر را انتخاب کنید (MTO-*.csv, Spools.csv, SpoolItems.csv)",
            "",  # مسیر پیش‌فرض
            "CSV Files (*.csv)"
        )

        if not file_paths:
            self.log_to_console("هیچ فایلی انتخاب نشد. عملیات لغو شد.", "warning")
            return

        self.log_to_console(f"شروع فرآیند به‌روزرسانی برای {len(file_paths)} فایل انتخابی...", "info")
        QApplication.processEvents()  # برای نمایش پیام قبل از شروع عملیات سنگین

        # --- CHANGE: فراخوانی تابع جدید و هوشمند در DataManager ---
        success, message = self.dm.process_selected_csv_files(file_paths)

        # ۵. نمایش نتیجه نهایی (بدون تغییر)
        if success:
            self.log_to_console(message, "success")
            self.show_message("موفق", message)
            self.populate_project_combo()  # لیست پروژه‌ها را برای نمایش تغییرات احتمالی، بازخوانی می‌کنیم
        else:
            self.log_to_console(message, "error")
            self.show_message("خطا", message, "error")

    def handle_iso_search(self):
        raw_line = (self.entries.get("Line No").text() if self.entries.get("Line No") else "").strip()
        if not raw_line:
            self.log_to_console("⚠️ لطفاً ابتدا Line No را وارد کنید.", level="warning")
            return

        try:
            # جستجو حالا بسیار ساده و سریع است و به هیچ پارامتر اضافه‌ای نیاز ندارد
            matches = self.dm.find_iso_files(raw_line)
        except Exception as e:
            self.log_to_console(f"❌ جستجوی فایل‌ها با خطا مواجه شد: {e}", level="error")
            return

        if not matches:
            self.log_to_console("⚠️ فایلی مطابق با Line No واردشده پیدا نشد.", level="warning")
            return

        self.log_to_console(f"✅ {len(matches)} فایل پیدا شد.", level="success")

        # --- بخش نمایش دیالوگ نتایج (بدون تغییر) ---
        dlg = QDialog(self)
        dlg.setWindowTitle("انتخاب و باز کردن فایل‌های ISO/DWG")
        dlg.resize(900, 500)

        v_layout = QVBoxLayout(dlg)
        info_label = QLabel("برای باز کردن فایل دوبار کلیک کنید یا روی «Open» بزنید.")
        v_layout.addWidget(info_label)

        table = QTableWidget(len(matches), 2, dlg)
        table.setHorizontalHeaderLabels(["File", "Folder"])
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        v_layout.addWidget(table)

        for r, path in enumerate(matches):
            name = os.path.basename(path)
            folder = os.path.dirname(path)
            table.setItem(r, 0, QTableWidgetItem(name))
            table.setItem(r, 1, QTableWidgetItem(folder))

        row_to_path = {i: p for i, p in enumerate(matches)}

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Open | QDialogButtonBox.StandardButton.Close,
                                   parent=dlg)
        v_layout.addWidget(buttons)

        def _open_selected():
            row = table.currentRow()
            if row < 0:
                return
            path = row_to_path.get(row)
            try:
                os.startfile(path)
                self.log_to_console(f"📂 فایل باز شد: {path}", level="info")
            except Exception as e:
                self.log_to_console(f"❌ خطا در باز کردن فایل {path}: {e}", level="error")

        buttons.button(QDialogButtonBox.StandardButton.Open).clicked.connect(_open_selected)
        buttons.rejected.connect(dlg.reject)
        table.cellDoubleClicked.connect(lambda *_: _open_selected())

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

    def open_spool_manager(self):
        # dlg = QInputDialog(self)
        # dlg.setWindowTitle("ورود رمز")
        # dlg.setLabelText("رمز را وارد کنید:")
        # dlg.setTextEchoMode(QLineEdit.EchoMode.Password)  # ⭐ نمایش به صورت ستاره
        # ok = dlg.exec()
        #
        # password = dlg.textValue()
        #
        # if not ok or password != self.dashboard_password:
        #     self.show_message("خطا", "رمز اشتباه است یا عملیات لغو شد.", "error")
        #     return

        # ✅ اگر رمز درست بود ادامه بده
        python_executable = sys.executable
        dialog = SpoolManagerDialog(self.dm, self)
        dialog.exec()

    def show_about_dialog(self):
        """پنجره اطلاعات مربوط به برنامه و سازنده را نمایش می‌دهد."""
        title = "About MIV Management"
        # استفاده از Rich Text (HTML) برای فرمت‌بندی و ایجاد لینک‌های قابل کلیک
        text = """
        <h2>Material Issue Tracker</h2>
        <p><b>Version:</b> 1.0.0</p>
        <p>This application helps track and manage Material Take-Off (MTO),
        Material Issue Vouchers (MIV), and Spool Inventory for engineering projects.</p>
        <hr>
        <p><b>Developer:</b> Hossein Izadi</p>
        <p><b>Email:</b> <a href="mailto:arkittoe@gmail.com">arkittoe@gmail.com</a></p>
        <p><b>GitHub Repository:</b> <a href="https://github.com/arkittioe/Material-Issue-Tracker-SQLDB">Material-Issue-Tracker-SQLDB</a></p>
        <br>
        <p><i>Built with Python, PyQt6, and SQLAlchemy.</i></p>
        """
        QMessageBox.about(self, title, text)

    def cleanup_processes(self):
        """کشتن کامل پروسه‌های جانبی و ترد نگهبان."""
        # ... کد قبلی برای بستن api_process و dashboard_process ...
        try:
            if hasattr(self, 'api_process') and self.api_process:
                self.api_process.kill()
            if hasattr(self, 'dashboard_process') and self.dashboard_process:
                self.dashboard_process.kill()

            # توقف ترد نگهبان
            if self.iso_observer:
                self.iso_observer.stop()
                self.iso_observer.join() # منتظر می‌مانیم تا ترد کاملا بسته شود
                print("ISO watcher stopped.")

        except Exception as e:
            print(f"⚠️ خطا در بستن پروسه‌ها: {e}")

    def update_iso_status_label(self, message, level):
        color_map = {"info": "#8be9fd", "success": "#50fa7b", "warning": "#f1fa8c", "error": "#ff5555"}
        color = color_map.get(level, "#f8f8f2")
        self.iso_status_label.setText(f"وضعیت ایندکس ISO: {message}")
        self.iso_status_label.setStyleSheet(f"padding: 4px; color: {color};")
        if level != "error":
            self.log_to_console(f"ISO Indexer: {message}", level)

    def start_iso_watcher(self):
        path = ISO_PATH  # مسیر را در صورت نیاز تغییر دهید
        if not os.path.isdir(path):
            self.update_iso_status_label(f"مسیر یافت نشد!", "error")
            return

        self.update_iso_status_label("در حال همگام‌سازی اولیه...", "warning")

        # --- CHANGE: ارسال کل event_handler به جای فقط سیگنال ---
        threading.Thread(target=self.dm.rebuild_iso_index_from_scratch,
                         args=(path, self.iso_event_handler), daemon=True).start()

        if self.iso_observer:
            self.iso_observer.stop()
            self.iso_observer.join()

        self.iso_observer = Observer()
        self.iso_observer.schedule(self.iso_event_handler, path, recursive=True)
        self.iso_observer.start()

    def update_iso_progress(self, value):
        """اسلات برای آپدیت کردن مقدار QProgressBar."""
        if value < 100:
            if not self.iso_progress_bar.isVisible():
                self.iso_progress_bar.show()
            self.iso_progress_bar.setValue(value)
        else:
            # وقتی کامل شد، آن را مخفی کرده و پیام موفقیت را نشان می‌دهیم
            self.iso_progress_bar.hide()
            self.iso_progress_bar.setValue(0)

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