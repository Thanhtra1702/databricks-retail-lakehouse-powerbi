# Power BI Semantic Model Design & Architecture

## 1. Data Model Overview
Mô hình dữ liệu được thiết kế theo chuẩn **Kimball Star Schema**, kết nối trực tiếp với tầng Gold của Databricks Lakehouse:

| Bảng | Loại bảng | Khóa chính (PK) / Khóa ngoại (FK) | Chế độ lưu trữ (Storage Mode) |
| :--- | :--- | :--- | :--- |
| **`fact_sales_daily`** | Fact Table | FK: `product_id`, `customer_id`, `order_date` | DirectQuery |
| **`dim_product`** | Dimension | PK: `product_id` | Dual / Import |
| **`dim_customer`** | Dimension | PK: `customer_id` | Dual / Import |
| **`dim_date`** | Dimension (DAX Generated) | PK: `Date` | Import |

## 2. Relationships (Quan hệ dữ liệu)
- `dim_product[product_id]` (1) $\rightarrow$ `fact_sales_daily[product_id]` (N): Active, Single Direction.
- `dim_customer[customer_id]` (1) $\rightarrow$ `fact_sales_daily[customer_id]` (N): Active, Single Direction.
- `dim_date[Date]` (1) $\rightarrow$ `fact_sales_daily[order_date]` (N): Active, Single Direction.

## 3. Storage Mode Strategy
- **Fact Table**: Thiết lập DirectQuery để tận dụng khả năng tính toán phân tán của Spark và bộ đệm Delta Cache trên Databricks.
- **Dimension Tables**: Thiết lập Dual/Import mode để bộ lọc slicer và visual phản hồi tức thì (latency < 100ms).