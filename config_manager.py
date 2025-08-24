import configparser
import os

# یک نمونه از ConfigParser ساخته می‌شود تا در کل برنامه استفاده شود
config = configparser.ConfigParser()

# مسیر فایل کانفیگ را به صورت دینامیک پیدا می‌کند
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')

# فایل را می‌خواند
config.read(config_path)

# استخراج مقادیر برای دسترسی آسان
ISO_PATH = config.get('Paths', 'iso_drawing_path', fallback=r'Y:\Piping\ISO')
DB_PATH = config.get('Paths', 'database_path', fallback='miv_registry.db')
DASHBOARD_PASSWORD = config.get('Security', 'dashboard_password', fallback='default_password')