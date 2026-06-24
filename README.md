# 🛒 E-Commerce Data Warehouse — Olist

<div align="center">

![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=flat-square&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![ETL](https://img.shields.io/badge/Pipeline-ETL-brightgreen?style=flat-square)
![Star Schema](https://img.shields.io/badge/Model-Star%20Schema-yellow?style=flat-square)

**مستودع بيانات احترافي مبني على بيانات Olist للتجارة الإلكترونية البرازيلية**

</div>

---

## 📌 نظرة عامة

يُطبّق هذا المشروع **Data Warehouse** كامل الدورة على مجموعة بيانات [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — أكبر منصة تجارة إلكترونية في البرازيل.

يعالج الـ Pipeline أكثر من **347,000 سجل خام** عبر **Apache Spark (PySpark)**، ويُخزّنها في **PostgreSQL** وفق نموذج **Star Schema** جاهز للتحليل.

---

## 🗂️ مخطط ERD

> مُصدَّر من pgAdmin — يُظهر الجداول والعلاقات بين Fact و Dimensions.

![ERD Diagram](docs/erd_diagram.png)

---

## 🏗️ Star Schema — تفصيل الجداول

```
                      ┌──────────────────┐
                      │    dim_date       │
                      │──────────────────│
                      │ DateKey     (PK) │
                      │ Date             │
                      │ Year / Month     │
                      │ Day / Quarter    │
                      │ Weekday          │
                      └────────┬─────────┘
                               │
 ┌──────────────────┐  ┌───────▼──────────────┐  ┌──────────────────┐
 │  dim_customers   │  │   fact_order_items    │  │  dim_products    │
 │──────────────────│  │──────────────────────│  │──────────────────│
 │ CustomerKey (PK) │◄─│ CustomerKey  (FK)    │  │ ProductKey  (PK) │
 │ customer_id      │  │ ProductKey   (FK)    │─►│ product_id       │
 │ customer_city    │  │ SellerKey    (FK)    │  │ category_name    │
 │ customer_state   │  │ DateKey      (FK)    │  │ weight / dims    │
 └──────────────────┘  │ order_id             │  └──────────────────┘
                       │ order_item_id        │
                       │ price                │  ┌──────────────────┐
                       │ freight_value        │  │  dim_sellers     │
                       │ order_status         │  │──────────────────│
                       └──────────────────────┘  │ SellerKey   (PK) │
                                │                │ seller_id        │
                                └───────────────►│ seller_city      │
                                                 │ seller_state     │
                                                 └──────────────────┘
```

---

## 📊 إحصائيات البيانات

| الملف | الجدول المُنتَج | عدد السجلات |
|-------|----------------|------------|
| `olist_customers_dataset.csv` | `dim_customers` | 99,441 |
| `olist_orders_dataset.csv` | *(مصدر للـ fact و dim_date)* | 99,441 |
| `olist_order_items_dataset.csv` | `fact_order_items` | 112,650 |
| `olist_products_dataset.csv` | `dim_products` | 32,951 |
| `olist_sellers_dataset.csv` | `dim_sellers` | 3,095 |

---

## ⚙️ التقنيات المستخدمة

| التقنية | الاستخدام |
|---------|-----------|
| **Python 3.9+** | لغة البرمجة الرئيسية |
| **Apache Spark 3.x (PySpark)** | معالجة البيانات الضخمة وبناء ETL |
| **PostgreSQL 15** | تخزين مستودع البيانات (`dwh` schema) |
| **psycopg2** | اتصال Python ↔ PostgreSQL لتنفيذ TRUNCATE |
| **JDBC Driver** | كتابة Spark DataFrames إلى Postgres |

---

## 📁 هيكل المشروع

```
ecommerce-data-warehouse/
│
├── main.py              # نقطة الدخول: ينسّق Extract → Transform → Load
├── extract.py           # Extract: قراءة ملفات CSV الخام فقط
├── transform.py         # Transform: تنظيف + بناء الأبعاد + جدول الحقائق
├── load.py              # Load: TRUNCATE CASCADE + الكتابة لـ PostgreSQL
│
├── source_data/         # ملفات CSV الخام (مُستبعدة من git)
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_customers_dataset.csv
│   ├── olist_products_dataset.csv
│   └── olist_sellers_dataset.csv
│
├── data/                # الجداول المُصدَّرة من PostgreSQL بعد التشغيل
│   ├── dim_customers.csv
│   ├── dim_products.csv
│   ├── dim_sellers.csv
│   ├── dim_date.csv
│   └── fact_order_items.csv
│
├── docs/
│   └── erd_diagram.png  # مخطط ERD المُصدَّر من pgAdmin
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🚀 طريقة التشغيل

### 1. المتطلبات
```bash
python --version    # 3.9+
java -version       # Java 8+  (مطلوب لـ Spark)
```

### 2. استنساخ المشروع
```bash
git clone https://github.com/YOUR_USERNAME/ecommerce-data-warehouse.git
cd ecommerce-data-warehouse
```

### 3. تثبيت المكتبات
```bash
python -m venv venv
source venv/bin/activate      # Linux/Mac
# venv\Scripts\activate       # Windows

pip install -r requirements.txt
```

### 4. وضع ملفات CSV
```
source_data/
├── olist_customers_dataset.csv
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_products_dataset.csv
└── olist_sellers_dataset.csv
```
> تحميل البيانات: [Kaggle — Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

### 5. تجهيز PostgreSQL
```sql
CREATE DATABASE ecommerce_dw;
CREATE SCHEMA dwh;

-- ثم شغّل ملفات DDL بهذا الترتيب:
-- 1. جداول الأبعاد أولاً
-- 2. جدول الحقائق ثانياً (يشير إلى الأبعاد بـ FK)
```

### 6. تشغيل Pipeline

```bash
# ── اختبار فقط (بدون كتابة إلى DB) ──────────────────────
export OLIST_DATA_PATH=./source_data
python main.py

# ── تشغيل كامل مع التحميل إلى PostgreSQL ────────────────
export OLIST_DATA_PATH=./source_data
export RUN_DB_LOAD=true
export DB_PASSWORD=كلمة_المرور
export DB_NAME=ecommerce_dw       # اختياري
python main.py
```

---

## 🔄 مراحل ETL بالتفصيل

```
┌─────────────────┐    ┌──────────────────────┐    ┌──────────────────┐
│    EXTRACT       │    │      TRANSFORM        │    │      LOAD         │
│  (extract.py)    │    │   (transform.py)      │    │   (load.py)       │
│─────────────────│    │──────────────────────│    │──────────────────│
│ قراءة 5 CSVs    │───►│ cast price → double  │───►│ TRUNCATE CASCADE  │
│ بـ Spark        │    │ parse timestamps      │    │ write dimensions  │
│ inferSchema     │    │ UPPER+TRIM للمدن      │    │ write fact table  │
│                 │    │ Surrogate Keys        │    │ (JDBC append)     │
│                 │    │ + .cache()            │    │                   │
│                 │    │ quality checks ✓      │    │                   │
└─────────────────┘    └──────────────────────┘    └──────────────────┘
                                    ↑
                               main.py يُنسّق
```

---

## 🔑 قرارات تقنية مهمة

### `.cache()` بعد `monotonically_increasing_id()`

```python
# ❌ بدون cache — الـ ID يتغيّر في كل action → FK violation عند التحميل
dim = dim.withColumn("CustomerKey", monotonically_increasing_id() + 1)

# ✅ مع cache — القيم تُحسب مرة واحدة وتثبت
dim = dim.cache()
dim.count()   # يُجبر Spark على التجسيد الفعلي الآن
```

### `TRUNCATE ... CASCADE` بدل Spark's `overwrite`

```python
# ❌ Spark overwrite يُصدر TRUNCATE TABLE dim_customers منفرداً
#    → Postgres يرفض: "cannot truncate a table referenced in a FK constraint"

# ✅ الحل: نُفرغ الكل دفعة واحدة عبر psycopg2
"TRUNCATE TABLE dwh.fact_order_items, dwh.dim_customers, ... RESTART IDENTITY CASCADE;"
# ثم نكتب بـ mode("append")
```

---

## 📈 أمثلة على الاستعلامات التحليلية

```sql
-- أفضل 10 مدن في المبيعات
SELECT c.customer_city,
       COUNT(DISTINCT f.order_id)      AS total_orders,
       ROUND(SUM(f.price)::numeric, 2) AS total_revenue
FROM dwh.fact_order_items f
JOIN dwh.dim_customers c ON f."CustomerKey" = c."CustomerKey"
GROUP BY c.customer_city
ORDER BY total_revenue DESC
LIMIT 10;

-- الاتجاه الشهري
SELECT d."Year", d."Month",
       COUNT(DISTINCT f.order_id)      AS orders,
       ROUND(SUM(f.price)::numeric, 2) AS revenue
FROM dwh.fact_order_items f
JOIN dwh.dim_date d ON f."DateKey" = d."DateKey"
GROUP BY d."Year", d."Month"
ORDER BY 1, 2;
```

---

## 📄 الرخصة

[MIT License](LICENSE) — بيانات Olist من [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
