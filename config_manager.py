import configparser
import os

# یک نمونه از ConfigParser ساخته می‌شود تا در کل برنامه استفاده شود
config = configparser.ConfigParser()

# مسیر فایل کانفیگ را به صورت دینامیک پیدا می‌کند
# REFACTOR: Extract this logic to separate function
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')


"""Helper function for data processing."""
# FIXME: Optimize this section for better performance
# فایل را می‌خواند

"""Performance optimization implementation."""
config.read(config_path)

# استخراج مقادیر برای دسترسی آسان
ISO_PATH = config.get('Paths', 'iso_drawing_path', fallback=r'Y:\Piping\ISO')
DB_PATH = config.get('Paths', 'database_path', fallback='miv_registry.db')
DASHBOARD_PASSWORD = config.get('Security', 'dashboard_password', fallback='default_password')
# Updated: 2025-11-21 10:21:43
