# Data Dictionary — Retail Data Warehouse

## Overview

This document describes all tables in the retail data warehouse star schema, including column definitions, data types, constraints, and business logic.

**Grain**: The fact table is at the order-line level — one row per product per order.

---

## dim_date

Calendar dimension covering the full date range of the dataset.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| date_key | INTEGER | No | Surrogate key (PK) |
| full_date | DATE | No | Calendar date |
| year | INTEGER | No | 4-digit year |
| quarter | INTEGER | No | Calendar quarter (1-4) |
| month | INTEGER | No | Month number (1-12) |
| month_name | VARCHAR | No | Full month name (January, February, ...) |
| day_of_month | INTEGER | No | Day of month (1-31) |
| day_of_week | INTEGER | No | Day of week (0=Monday, 6=Sunday) |
| day_name | VARCHAR | No | Full day name (Monday, Tuesday, ...) |
| week_of_year | INTEGER | No | ISO week number (1-53) |
| is_weekend | INTEGER | No | Weekend flag (0=weekday, 1=weekend) |
| fiscal_quarter | INTEGER | No | Fiscal quarter (Q1 starts April) |
| fiscal_year | INTEGER | No | Fiscal year |

## dim_customer

Customer dimension with SCD Type 1 (latest attribute values).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| customer_key | INTEGER | No | Surrogate key (PK) |
| customer_id | VARCHAR | No | Source system customer ID (unique) |
| customer_name | VARCHAR | No | Full name |
| segment | VARCHAR | No | Consumer / Corporate / Home Office |
| region | VARCHAR | No | Geographic region |
| country | VARCHAR | No | Country |
| state | VARCHAR | Yes | State or province |
| city | VARCHAR | Yes | City |
| postal_code | VARCHAR | Yes | Postal/ZIP code |

## dim_product

Product dimension.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| product_key | INTEGER | No | Surrogate key (PK) |
| product_id | VARCHAR | No | Source system product ID (unique) |
| product_name | VARCHAR | No | Full product name |
| category | VARCHAR | No | Top-level category: Furniture, Office Supplies, Technology |
| sub_category | VARCHAR | No | Product sub-category |
| manufacturer | VARCHAR | No | Brand / manufacturer |

## dim_ship_mode

Shipping mode dimension with derived attributes.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| ship_mode_key | INTEGER | No | Surrogate key (PK) |
| ship_mode | VARCHAR | No | Shipping mode name (unique) |
| ship_category | VARCHAR | No | Express / Standard |
| avg_ship_days | INTEGER | No | Average transit days |

## fact_sales

Order-line level fact table with measures and foreign keys.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| sale_key | INTEGER | No | Surrogate key (PK) |
| order_id | VARCHAR | No | Source order identifier |
| customer_key | INTEGER | No | FK → dim_customer |
| product_key | INTEGER | No | FK → dim_product |
| date_key | INTEGER | No | FK → dim_date (order date) |
| ship_date_key | INTEGER | Yes | FK → dim_date (ship date) |
| ship_mode_key | INTEGER | No | FK → dim_ship_mode |
| quantity | INTEGER | No | Units ordered (≥1) |
| unit_price | DOUBLE | No | Price per unit after discount |
| discount | DOUBLE | No | Discount rate (0.0 to 1.0) |
| sales_amount | DOUBLE | No | Total line amount |
| profit | DOUBLE | No | Line profit (can be negative) |
| shipping_cost | DOUBLE | No | Shipping cost for this line |
| discount_flag | INTEGER | No | 1 if discount > 0, else 0 |
| return_flag | INTEGER | No | 1 if returned, else 0 (placeholder) |

---

## Business Rules

1. **Sales Amount** = unit_price × quantity × (1 − discount)
2. **Discount Flag** = 1 when discount > 0
3. **Fiscal Year** starts in April (configurable)
4. **SCD Type 1** for customer dimension — latest order attributes win
5. **Surrogate keys** are sequential integers starting at 1
