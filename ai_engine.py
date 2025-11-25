# file: ai_engine.py

import pandas as pd
from sklearn.ensemble import IsolationForest
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules
from prophet import Prophet
import joblib
import os
import numpy as np
from datetime import datetime, timedelta
import warnings

# نادیده گرفتن هشدارهای مربوط به Prophet
warnings.simplefilter("ignore", DeprecationWarning)
warnings.simplefilter("ignore", FutureWarning)


# --- 1. سیستم پیشنهادگر هوشمند (نسخه بهبودیافته) ---
class Recommender:
    """
    (نسخه بهبودیافته)
    این کلاس با استفاده از الگوریتم Apriori، یاد می‌گیرد که کدام آیتم‌ها
    معمولاً با هم در یک MIV مصرف می‌شوند.
    بهبود اصلی: این مدل دیگر فقط یک مدل کلی نیست، بلکه می‌تواند برای گروه‌های مختلف
    (مثلاً هر پروژه) یک مدل مجزا آموزش ببیند تا پیشنهادهای مرتبط‌تری ارائه دهد.
    """

    def __init__(self, model_path='recommender_rules.pkl'):
        self.model_path = model_path
        self.rules = {}  # دیکشنری برای نگهداری قوانین به تفکیک گروه
        if os.path.exists(self.model_path):
            self.load_model()

    def train(self, transactions_by_group: dict[str, list[list[str]]], logger=print):
        """
        مدل را بر اساس تاریخچه تمام MIV ها به تفکیک گروه آموزش می‌دهد.
        """
        if not transactions_by_group:
            logger("تراکنش کافی برای آموزش مدل پیشنهادگر وجود ندارد.", "warning") # <<< CHANGE
            return

        for group_key, transactions in transactions_by_group.items():
            if len(transactions) < 10:
                continue

            te = TransactionEncoder()
            te_ary = te.fit(transactions).transform(transactions)
            df = pd.DataFrame(te_ary, columns=te.columns_)

            frequent_itemsets = apriori(df, min_support=0.05, use_colnames=True)
            if frequent_itemsets.empty:
                continue

            rules_df = association_rules(frequent_itemsets, metric="lift", min_threshold=1.1)
            self.rules[group_key] = rules_df

        self.save_model()
        logger(f"✅ مدل پیشنهادگر برای {len(self.rules)} گروه با موفقیت آموزش دید و ذخیره شد.", "success")  # <<< CHANGE

    def load_model(self, logger=print):
        self.rules = joblib.load(self.model_path)
        logger(f"مدل پیشنهادگر برای {len(self.rules)} گروه از فایل بارگذاری شد.", "info")  # <<< CHANGE

    def recommend(self, items: list[str], group_key: str = 'global', top_n=5) -> list[str]:
        """
        بر اساس آیتم‌های انتخاب شده در یک گروه خاص، بهترین آیتم‌های بعدی را پیشنهاد می‌دهد.
        """
        rules_df = self.rules.get(group_key)
        if rules_df is None or items is None or not items:
            return []

        recommendations = rules_df[rules_df['antecedents'].apply(lambda x: set(x).issubset(set(items)))]
        if recommendations.empty:
            return []

        # مرتب‌سازی بر اساس 'lift' (میزان جذابیت قانون) و 'confidence'
        recommendations = recommendations.sort_values(by=['lift', 'confidence'], ascending=False)

        consequents = recommendations['consequents'].explode().unique().tolist()
        final_recs = [rec for rec in consequents if rec not in items]

        return final_recs[:top_n]

    def save_model(self):
        joblib.dump(self.rules, self.model_path)


# --- 2. پیش‌بینی کسری متریال (نسخه بهبودیافته با Prophet) ---
class ShortagePredictor:
    """
    (نسخه بهبودیافته)
    برای هر آیتم، یک مدل سری زمانی (Time Series) با استفاده از کتابخانه Prophet آموزش می‌دهد
    تا الگوهای مصرف غیرخطی (مانند S-Curve در پروژه‌ها) را یاد بگیرد و پیش‌بینی کند
    موجودی چه زمانی تمام می‌شود.
    """

    def __init__(self, model_path='shortage_predictor_prophet.pkl'):
        self.model_path = model_path
        self.models = {}
        if os.path.exists(self.model_path):
            self.load_model()

    def train(self, consumption_df: pd.DataFrame, logger=print):
        """
        برای هر آیتم یک مدل Prophet مجزا آموزش می‌دهد.
        consumption_df: باید ستون‌های ['item_code', 'timestamp', 'used_qty'] را داشته باشد.
        """
        if consumption_df.empty:
            logger("داده مصرفی برای آموزش مدل پیش‌بینی کسری یافت نشد.", "warning")  # <<< CHANGE
            return

        consumption_df['timestamp'] = pd.to_datetime(consumption_df['timestamp'])
        all_item_codes = consumption_df['item_code'].unique()

        for item_code in all_item_codes:
            item_df = consumption_df[consumption_df['item_code'] == item_code].copy()
            if len(item_df) < 10:  # حداقل ۱۰ نقطه داده برای مدل سری زمانی
                continue

            # آماده‌سازی داده برای Prophet (ستون‌ها باید 'ds' و 'y' باشند)
            item_df = item_df.sort_values('timestamp')
            item_df['cumulative_qty'] = item_df['used_qty'].cumsum()
            prophet_df = item_df[['timestamp', 'cumulative_qty']].rename(
                columns={'timestamp': 'ds', 'cumulative_qty': 'y'}
            )

            # آموزش مدل Prophet
            model = Prophet(yearly_seasonality=False, weekly_seasonality=True, daily_seasonality=False)
            model.fit(prophet_df)
            self.models[item_code] = model

        self.save_model()
        logger(f"✅ مدل پیش‌بینی کسری (Prophet) برای {len(self.models)} آیتم آموزش دید و ذخیره شد.", "success") # <<< CHANGE

    def predict(self, item_code: str, total_required: float, current_used: float) -> (str | None):
        """
        پیش‌بینی می‌کند که یک آیتم خاص در چه تاریخی تمام خواهد شد.
        """
        if item_code not in self.models:
            return None

        model = self.models[item_code]

        # اگر مصرف فعلی بیشتر از مورد نیاز باشد، پیش‌بینی نکن
        if current_used >= total_required:
            return None

        # ساخت دیتافریم آینده برای پیش‌بینی
        future = model.make_future_dataframe(periods=730)  # پیش‌بینی تا ۲ سال آینده
        forecast = model.predict(future)

        try:
            # پیدا کردن اولین روزی که مصرف پیش‌بینی‌شده ('yhat') از کل مورد نیاز بیشتر می‌شود
            shortage_forecast = forecast[forecast['yhat'] >= total_required]

            if shortage_forecast.empty:
                # اگر با روند فعلی هیچوقت تمام نمی‌شود
                return "هرگز (مصرف بسیار کند)"

            predicted_date = shortage_forecast['ds'].min().to_pydatetime()

            if predicted_date < datetime.now():
                return "اکنون (مصرف کندتر از پیش‌بینی)"

            return predicted_date.strftime('%Y-%m-%d')

        except Exception:
            return None

    def save_model(self):
        joblib.dump(self.models, self.model_path)

    def load_model(self, logger=print):
        self.models = joblib.load(self.model_path)
        logger("مدل پیش‌بینی کسری (Prophet) از فایل بارگذاری شد.", "info")  # <<< CHANGE

# --- 3. شناسایی ناهنجاری (نسخه بهبودیافته) ---
class AnomalyDetector:
    """
    (نسخه بهبودیافته)
    با استفاده از الگوریتم IsolationForest، یاد می‌گیرد که یک رکورد مصرف "عادی"
    چه شکلی است.
    بهبود اصلی: نرمال‌سازی داده در مرحله پیش‌بینی به صورت صحیح با استفاده از آمار
    داده‌های آموزشی انجام می‌شود و امکان افزودن ویژگی‌های غنی‌تر فراهم شده است.
    """

    def __init__(self, model_path='anomaly_detector.pkl'):
        self.model_path = model_path
        self.model = None
        self.training_stats = {}  # برای ذخیره میانگین و انحراف معیار
        if os.path.exists(self.model_path):
            self.load_model()

    def train(self, miv_df: pd.DataFrame, logger=print):
        """
        مدل را بر اساس تاریخچه تمام MIV ها آموزش می‌دهد.
        miv_df: باید ستون‌های ['used_qty', 'total_qty', 'timestamp'] را داشته باشد.
        """
        if miv_df.empty or len(miv_df) < 10:
            logger("داده کافی برای آموزش مدل شناسایی ناهنجاری وجود ندارد.", "warning")  # <<< CHANGE
            return

        df = miv_df.copy()

        # --- مهندسی ویژگی ---
        df['usage_ratio'] = (df['used_qty'] / df['total_qty']).replace([np.inf, -np.inf], 0).fillna(0)
        df['hour_of_day'] = pd.to_datetime(df['timestamp']).dt.hour

        # محاسبه و ذخیره آمار برای نرمال‌سازی
        mean_qty = df['used_qty'].mean()
        std_qty = df['used_qty'].std()
        self.training_stats['mean_qty'] = mean_qty
        self.training_stats['std_qty'] = std_qty

        df['normalized_qty'] = (df['used_qty'] - mean_qty) / std_qty if std_qty > 0 else 0

        # افزودن ویژگی‌های جدید (در صورت تمایل می‌توانید این بخش را کامل‌تر کنید)
        # df['day_of_week'] = pd.to_datetime(df['timestamp']).dt.dayofweek
        # df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)

        features = df[['usage_ratio', 'hour_of_day', 'normalized_qty']].fillna(0)

        self.model = IsolationForest(contamination=0.02, random_state=42)
        self.model.fit(features)
        self.save_model()
        logger("✅ مدل شناسایی ناهنجاری آموزش دید و ذخیره شد.", "success") # <<< CHANGE


    def predict(self, data_point: pd.DataFrame) -> bool:
        """
        یک رکورد جدید را بررسی کرده و مشخص می‌کند که ناهنجار است یا خیر.
        """
        if self.model is None or not self.training_stats:
            return False

        df = data_point.copy()

        # آماده‌سازی ویژگی‌ها دقیقاً مانند مرحله آموزش
        df['usage_ratio'] = (df['used_qty'] / df['total_qty']).replace([np.inf, -np.inf], 0).fillna(0)
        df['hour_of_day'] = pd.to_datetime(df['timestamp']).dt.hour

        # **اصلاح کلیدی:** استفاده از آمار داده‌های آموزشی برای نرمال‌سازی
        mean_qty = self.training_stats['mean_qty']
        std_qty = self.training_stats['std_qty']
        df['normalized_qty'] = (df['used_qty'] - mean_qty) / std_qty if std_qty > 0 else 0

        features = df[['usage_ratio', 'hour_of_day', 'normalized_qty']].fillna(0)

        prediction = self.model.predict(features)
        return prediction[0] == -1

    def save_model(self):
        # ذخیره مدل و آمار با هم در یک فایل
        data_to_save = {'model': self.model, 'stats': self.training_stats}
        joblib.dump(data_to_save, self.model_path)

    def load_model(self, logger=print):
        data_loaded = joblib.load(self.model_path)
        self.model = data_loaded['model']
        self.training_stats = data_loaded['stats']
        logger("مدل شناسایی ناهنجاری و آمار آموزشی از فایل بارگذاری شد.", "info") # <<< CHANGEذاری شد.")
# Updated: 2025-11-25 07:32:47
