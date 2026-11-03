-- =========================================================================
-- SCRIPT: 02_security_governance.sql
-- DESCRIPTION: Unity Catalog Row-Level Security (RLS) & Column Masking
-- =========================================================================

USE CATALOG omni_retail_catalog;
USE SCHEMA gold;

-- 1. Dynamic Data Masking cho Email khách hàng PII
-- Nhóm 'data_admins' thấy email đầy đủ; các role khác bị che thông tin
CREATE OR REPLACE FUNCTION mask_customer_email(email STRING)
RETURN CASE 
    WHEN is_account_group_member('data_admins') THEN email
    ELSE CONCAT(LEFT(email, 2), '****@', SPLIT(email, '@')[1])
END;

ALTER TABLE dim_customer 
ALTER COLUMN email SET MASK mask_customer_email;

-- 2. Row-Level Security (RLS) phân quyền dữ liệu theo vị trí địa lý
-- Cấp quản lý xem toàn bộ; nhân viên chi nhánh chỉ xem dữ liệu trong bang (State) được phân quyền
CREATE OR REPLACE FUNCTION state_row_filter(state STRING)
RETURN 
    is_account_group_member('executives') 
    OR state = 'CA';

ALTER TABLE dim_customer 
SET ROW FILTER state_row_filter ON (state);