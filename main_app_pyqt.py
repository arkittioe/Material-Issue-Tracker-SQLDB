# main_app_pyqt.py

import sys

import webbrowser
import subprocess

import os

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QFormLayout,
    QLabel, QComboBox, QPushButton, QTextEdit, QFrame, QMessageBox, QLineEdit,
    QTableWidget, QTableWidgetItem, QHeaderView, QDialog, QDialogButtonBox, QDoubleSpinBox, QSplitter,
    QCompleter, QInputDialog # 🔹 ویجت کامپلیتر را وارد کنید
)

from PyQt6.QtGui import QFont, QColor
from PyQt6.QtCore import Qt, QStringListModel

# برای نمایش نمودار در PyQt6
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

# فرض بر این است که این دو فایل در کنار این اسکریپت قرار دارند
from data_manager import DataManager
from models import Project, MTOItem, MIVRecord  # برای type hinting


class MTOConsumptionDialog(QDialog):
    def __init__(self, dm: DataManager, project_id: int, line_no: str, miv_record_id: int = None, parent=None):
        super().__init__(parent)
        self.dm = dm
        self.project_id = project_id
        self.line_no = line_no
        self.miv_record_id = miv_record_id
        self.consumed_data = []
        self.existing_consumptions = {}

        self.setWindowTitle(f"ویرایش مصرف برای خط: {self.line_no}")
        self.setMinimumSize(900, 500)

        if self.miv_record_id:
            self.setWindowTitle(f"ویرایش آیتم‌های MIV ID: {self.miv_record_id}")
            self.existing_consumptions = self.dm.get_consumptions_for_miv(self.miv_record_id)

        layout = QVBoxLayout(self)
        info_label = QLabel("مقدار مصرف جدید را برای هر آیتم وارد کنید. مقدار 0 به معنی حذف آیتم از این MIV است.")
        layout.addWidget(info_label)

        self.table = QTableWidget()
        self.table.setColumnCount(7)
        self.table.setHorizontalHeaderLabels([
            "Item Code", "Description", "Total Qty", "Used Qty (All)", "Remaining Qty", "Unit", "مقدار مصرف این MIV"
        ])

        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)

        layout.addWidget(self.table)

        self.populate_table()

        # این دو خط حذف شدند تا ستون‌ها با اندازه پیش‌فرض نمایش داده شوند
        # self.table.resizeColumnsToContents()
        # self.table.resizeRowsToContents()

        self.buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def populate_table(self):
        # ... محتوای این تابع بدون تغییر باقی می‌ماند ...
        self.progress_data = self.dm.get_line_material_progress(self.project_id, self.line_no, readonly=False)
        self.table.setRowCount(len(self.progress_data))
        for row_idx, item in enumerate(self.progress_data):
            current_miv_usage = self.existing_consumptions.get(item["mto_item_id"], 0)
            remaining_qty = item["Remaining Qty"] or 0
            self.table.setItem(row_idx, 0, QTableWidgetItem(item["Item Code"] or ""))
            self.table.setItem(row_idx, 1, QTableWidgetItem(item["Description"] or ""))
            self.table.setItem(row_idx, 2, QTableWidgetItem(str(item["Total Qty"] or 0)))
            self.table.setItem(row_idx, 3, QTableWidgetItem(str(item["Used Qty"] or 0)))
            self.table.setItem(row_idx, 4, QTableWidgetItem(str(remaining_qty)))
            self.table.setItem(row_idx, 5, QTableWidgetItem(item["Unit"] or ""))
            spin_box = QDoubleSpinBox()
            max_val = remaining_qty + current_miv_usage
            spin_box.setRange(0, max_val)
            spin_box.setDecimals(2)
            spin_box.setValue(current_miv_usage)
            if max_val == 0 and current_miv_usage == 0:
                spin_box.setEnabled(False)
                for col in range(6):
                    self.table.item(row_idx, col).setBackground(QColor("#d3d3d3"))
            self.table.setCellWidget(row_idx, 6, spin_box)
            for col_idx in range(6):
                item_widget = self.table.item(row_idx, col_idx)
                item_widget.setFlags(item_widget.flags() & ~Qt.ItemFlag.ItemIsEditable)

    def accept(self):
        # ... محتوای این تابع بدون تغییر باقی می‌ماند ...
        for row_idx, item in enumerate(self.progress_data):
            spin_box = self.table.cellWidget(row_idx, 6)
            consumed_qty = spin_box.value()
            if consumed_qty > 0:
                self.consumed_data.append({
                    "mto_item_id": item["mto_item_id"],
                    "used_qty": consumed_qty,
                    "item_code": item["Item Code"],
                    "description": item["Description"],
                    "unit": item["Unit"]
                })
        super().accept()

    def get_consumed_data(self):
        # ... محتوay این تابع بدون تغییر باقی می‌ماند ...
        return self.consumed_data

# --- پنجره اصلی برنامه ---
class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("مدیریت MIV - نسخه PyQt6")
        self.setGeometry(100, 100, 1200, 800)

        self.dm = DataManager(db_path="miv_registry.db")
        self.current_project: Project | None = None
        self.current_user = os.getlogin()  # گرفتن نام کاربری سیستم
        self.suggestion_data = [] # 🔹 برای نگهداری داده‌های کامل پیشنهادها

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
        # -- خط اصلاح شده در زیر --
        # با استفاده از keyword argument یعنی parent=self مشکل حل می‌شود.
        # حالا miv_record_id مقدار پیش‌فرض خود یعنی None را می‌گیرد.
        dialog = MTOConsumptionDialog(self.dm, self.current_project.id, form_data["Line No"], parent=self)

        if dialog.exec():
            consumed_items = dialog.get_consumed_data()
            if not consumed_items:
                self.log_to_console("ثبت رکورد لغو شد چون هیچ آیتمی مصرف نشده بود.", "warning")
                return

            # ساخت کامنت
            comment_parts = [
                f"{item['used_qty']} * {(item.get('item_code') or item['description'])}"
                for item in consumed_items
            ]

            form_data["Comment"] = ", ".join(comment_parts)

            # ثبت نهایی
            success, msg = self.dm.register_miv_record(self.current_project.id, form_data, consumed_items)
            if success:
                self.log_to_console(msg, "success")
                self.update_line_dashboard()
                for widget in self.entries.values():
                    widget.clear()
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

        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)

        table.setRowCount(len(records))

        for row, rec in enumerate(records):
            table.setItem(row, 0, QTableWidgetItem(str(rec.id)))
            table.setItem(row, 1, QTableWidgetItem(rec.miv_tag or ""))
            table.setItem(row, 2, QTableWidgetItem(rec.location or ""))
            table.setItem(row, 3, QTableWidgetItem(rec.status or ""))
            table.setItem(row, 4, QTableWidgetItem(rec.comment or ""))
            table.setItem(row, 5, QTableWidgetItem(rec.registered_for or ""))
            table.setItem(row, 6, QTableWidgetItem(rec.registered_by or ""))
            table.setItem(row, 7, QTableWidgetItem(str(rec.last_updated) if rec.last_updated else ""))

        layout.addWidget(table)

        # این دو خط حذف شدند تا ستون‌ها با اندازه پیش‌فرض نمایش داده شوند
        # table.resizeColumnsToContents()
        # table.resizeRowsToContents()

        btn_layout = QHBoxLayout()
        edit_btn = QPushButton("✏️ ویرایش رکورد")
        delete_btn = QPushButton("🗑️ حذف رکórd")
        edit_items_btn = QPushButton("✏️ ویرایش آیتم‌های مصرفی")
        close_btn = QPushButton("بستن")

        # ... بقیه کد تابع handle_search بدون تغییر باقی می‌ماند ...
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
                consumed_items = dialog.get_consumed_data()
                success, msg = self.dm.update_miv_items(record_id, consumed_items, user=self.current_user)
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

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())

