# file: models.py

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime

Base = declarative_base()

# -------------------------
# جدول پروژه‌ها
# -------------------------
class Project(Base):
    __tablename__ = 'projects'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    miv_records = relationship("MIVRecord", back_populates="project")
    mto_items = relationship("MTOItem", back_populates="project")


# -------------------------
# جدول MIV Records
# -------------------------
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
    last_updated = Column(DateTime, default=datetime.utcnow)  # همه رکوردهای جدید تاریخ امروز
    is_complete = Column(Boolean, default=False)

    project = relationship("Project", back_populates="miv_records")


# -------------------------
# جدول MTO Items
# -------------------------
class MTOItem(Base):
    __tablename__ = 'mto_items'
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=False)
    unit = Column(String)            # UNIT
    line_no = Column(String, nullable=False)
    item_class = Column(String)      # Class
    item_type = Column(String)       # Type
    description = Column(String)
    item_code = Column(String)
    material_code = Column(String)   # Mat.
    p1_bore_in = Column(Float)
    p2_bore_in = Column(Float)
    p3_bore_in = Column(Float)
    length_m = Column(Float)
    quantity = Column(Float)
    joint = Column(Float)
    inch_dia = Column(Float)

    project = relationship("Project", back_populates="mto_items")


# -------------------------
# جدول MTO Progress
# -------------------------
class MTOProgress(Base):
    __tablename__ = 'mto_progress'
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=False)
    line_no = Column(String, nullable=False)
    mto_item_id = Column(Integer, ForeignKey('mto_items.id'), nullable=False)  # 🔹 اضافه شد
    item_code = Column(String)
    description = Column(String)
    unit = Column(String)
    total_qty = Column(Float)
    used_qty = Column(Float)
    remaining_qty = Column(Float)
    last_updated = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('project_id', 'line_no', 'item_code', 'mto_item_id', name='uq_progress_item'),  # ✅ کلید یکتا
    )



# -------------------------
# جدول MTO Consumption
# -------------------------
class MTOConsumption(Base):
    __tablename__ = 'mto_consumption'
    id = Column(Integer, primary_key=True)
    mto_item_id = Column(Integer, ForeignKey('mto_items.id'), nullable=False)
    miv_record_id = Column(Integer, ForeignKey('miv_records.id'), nullable=False)
    used_qty = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)


# -------------------------
# جدول Activity Log
# -------------------------
class ActivityLog(Base):
    __tablename__ = 'activity_logs'
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    user = Column(String)
    action = Column(String)
    details = Column(String)


# -------------------------
# جدول Migrated Files
# -------------------------
class MigratedFile(Base):
    __tablename__ = 'migrated_files'
    id = Column(Integer, primary_key=True)
    filename = Column(String, unique=True, nullable=False)
    migrated_at = Column(DateTime, default=datetime.utcnow)


# -------------------------
# تابع ایجاد دیتابیس و جداول
# -------------------------
def setup_database():
    engine = create_engine('sqlite:///miv_registry.db')
    Base.metadata.create_all(engine)
    print("Database and tables created successfully.")


# -------------------------
if __name__ == '__main__':
    setup_database()
