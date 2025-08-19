# file: FILEDB.py
# این فایل فقط برای اینه که یه بار خروجی بگیرم از فایل های csv ام و تبدیلش کنم به دیتا بیسم  برای یک بار

import os
import pandas as pd
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Float, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from datetime import datetime, timezone

Base = declarative_base()

# -------------------------
# مدل‌ها
# -------------------------
class Project(Base):
    __tablename__ = 'projects'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    miv_records = relationship("MIVRecord", back_populates="project")
    mto_items = relationship("MTOItem", back_populates="project")

class MIVRecord(Base):
    __tablename__ = 'miv_records'
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=False)
    line_no = Column(String, nullable=False)
    miv_tag = Column(String, unique=True)
    location = Column(String)
    status = Column(String)
    comment = Column(String)
    registered_for = Column(String)
    registered_by = Column(String)
    last_updated = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    is_complete = Column(Boolean, default=False)

    project = relationship("Project", back_populates="miv_records")

class MTOItem(Base):
    __tablename__ = 'mto_items'
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=False)
    unit = Column(String)
    line_no = Column(String, nullable=False)
    item_class = Column(String)
    item_type = Column(String)
    description = Column(String)
    item_code = Column(String)
    material_code = Column(String)
    p1_bore_in = Column(Float)
    p2_bore_in = Column(Float)
    p3_bore_in = Column(Float)
    length_m = Column(Float)
    quantity = Column(Float)
    joint = Column(Float)
    inch_dia = Column(Float)

    project = relationship("Project", back_populates="mto_items")

class MTOProgress(Base):
    __tablename__ = 'mto_progress'
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=False)  # ✅ اضافه شد
    mto_item_id = Column(Integer, ForeignKey('mto_items.id'), nullable=False)
    line_no = Column(String, nullable=False)
    item_code = Column(String)
    description = Column(String)
    unit = Column(String)
    total_qty = Column(Float)
    used_qty = Column(Float)
    remaining_qty = Column(Float)
    last_updated = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('project_id', 'line_no', 'item_code', 'mto_item_id', name='uq_progress_item'),  # ✅ کلید یکتا
    )

class MTOConsumption(Base):
    __tablename__ = 'mto_consumption'
    id = Column(Integer, primary_key=True)
    mto_item_id = Column(Integer, ForeignKey('mto_items.id'), nullable=False)
    miv_record_id = Column(Integer, ForeignKey('miv_records.id'), nullable=False)
    used_qty = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class ActivityLog(Base):
    __tablename__ = 'activity_logs'
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    user = Column(String)
    action = Column(String)
    details = Column(String)

class MigratedFile(Base):
    __tablename__ = 'migrated_files'
    id = Column(Integer, primary_key=True)
    filename = Column(String, unique=True, nullable=False)
    migrated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# -------------------------
# ایجاد دیتابیس و جداول
# -------------------------
def setup_database():
    engine = create_engine('sqlite:///miv_registry.db')
    Base.metadata.create_all(engine)
    print("Database and tables created successfully.")

# -------------------------
# تابع مهاجرت MTO و MIV
# -------------------------
def migrate_files():
    engine = create_engine('sqlite:///miv_registry.db')
    Session = sessionmaker(bind=engine)
    session = Session()

    from sqlalchemy import inspect
    inspector = inspect(engine)
    if not inspector.has_table(MigratedFile.__tablename__):
        Base.metadata.tables[MigratedFile.__tablename__].create(bind=engine)

    # لیست فایل‌های CSV
    all_files = [f for f in os.listdir('.') if f.endswith('.csv')]
    migrated_files = {f.filename for f in session.query(MigratedFile).all()}
    files_to_migrate = [f for f in all_files if f not in migrated_files]

    if not files_to_migrate:
        print("تمام فایل‌ها قبلاً منتقل شده‌اند.")
        session.close()
        return

    for file in files_to_migrate:
        df = pd.read_csv(file)
        # فقط نام پروژه واقعی بدون پسوند یا شماره پیشوند
        project_name = file.split('-')[-1].replace('.csv', '').strip()
        project = session.query(Project).filter_by(name=project_name).first()
        if not project:
            project = Project(name=project_name)
            session.add(project)
            session.commit()

        # ---------- MTO-PXX.csv ----------
        if file.startswith('MTO-P') and 'PROGRESS' not in file:
            items = []
            for _, row in df.iterrows():
                items.append(MTOItem(
                    project_id=project.id,
                    unit=row.get('UNIT'),
                    line_no=str(row.get('Line No') or '').strip(),
                    item_class=row.get('Class'),
                    item_type=row.get('Type'),
                    description=row.get('Description'),
                    item_code=row.get('Itemcode'),
                    material_code=row.get('Mat.'),
                    p1_bore_in=row.get('P1BORE(IN)'),
                    p2_bore_in=row.get('P2BORE(IN)'),
                    p3_bore_in=row.get('P3BORE(IN)'),
                    length_m=row.get('LENGTH(M)'),
                    quantity=row.get('QUANTITY'),
                    joint=row.get('JOINT'),
                    inch_dia=row.get('INCH DIA')
                ))
            session.bulk_save_objects(items)
            session.commit()

        # ---------- PXX.csv ----------
        elif 'MTO' not in file:
            for _, row in df.iterrows():
                miv = MIVRecord(
                    project_id=project.id,
                    line_no=row.get('Line No'),
                    miv_tag=row.get('MIV Tag'),
                    location=row.get('Location'),
                    status=row.get('Status'),
                    comment=row.get('Comment'),
                    registered_for=row.get('Registered For'),
                    registered_by=row.get('Registered By'),
                    last_updated=datetime.now(timezone.utc),
                    is_complete=row.get('Complete') in [True, 'TRUE', 'True', 1]
                )
                session.add(miv)
            session.commit()

        # ---------- MTO_PROGRESS-PXX.csv ----------
        # ---------- MTO_PROGRESS-PXX.csv ----------
        elif 'MTO_PROGRESS' in file:
            for _, row in df.iterrows():
                line_no = str(row.get('Line No') or '').strip()
                item_code = str(row.get('Item Code') or '').strip()

                # پیدا کردن MTOItem
                mto_item = session.query(MTOItem).filter_by(
                    project_id=project.id,
                    line_no=line_no,
                    item_code=item_code
                ).first()

                if not mto_item:
                    print(f"خطا: MTOItem پیدا نشد برای Line No={line_no} و Item Code={item_code}")
                    continue  # از اضافه کردن این رکورد عبور کن

                prog = MTOProgress(
                    project_id=project.id,
                    line_no=line_no,
                    mto_item_id=mto_item.id,
                    item_code=item_code,
                    description=row.get('Description'),
                    unit=row.get('Unit'),
                    total_qty=row.get('Total Qty'),
                    used_qty=row.get('Used Qty'),
                    remaining_qty=row.get('Remaining Qty'),
                    last_updated=datetime.now(timezone.utc)
                )
                session.add(prog)
            session.commit()


        # ثبت نام فایل منتقل شده و لاگ
        migrated_file = MigratedFile(filename=file)
        session.add(migrated_file)
        session.commit()

        log = ActivityLog(
            user='system',
            action='FILE_MIGRATION',
            details=f'File {file} migrated successfully.'
        )
        session.add(log)
        session.commit()

        print(f"File {file} migrated successfully.")

    session.close()
    print("Migration complete.")

# -------------------------
if __name__ == '__main__':
    setup_database()
    migrate_files()
