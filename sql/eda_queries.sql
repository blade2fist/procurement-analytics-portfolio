-- ============================================
-- Procurement Analytics SQL Queries
-- ============================================

-- 1. Общее количество контрактов и общая сумма
SELECT
    COUNT(*) AS contracts_count,
    SUM(contract_price) AS total_contract_value,
    AVG(contract_price) AS avg_contract_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY contract_price) AS median_contract_value
FROM contracts;


-- 2. Топ-10 заказчиков по сумме контрактов
SELECT
    customer_name,
    COUNT(*) AS contracts_count,
    SUM(contract_price) AS total_contract_value
FROM contracts
GROUP BY customer_name
ORDER BY total_contract_value DESC
LIMIT 10;


-- 3. Топ-10 поставщиков по сумме контрактов
SELECT
    supplier_name,
    COUNT(*) AS contracts_count,
    SUM(contract_price) AS total_contract_value
FROM contracts
GROUP BY supplier_name
ORDER BY total_contract_value DESC
LIMIT 10;


-- 4. Способы закупки по количеству и сумме контрактов
SELECT
    purchase_method,
    COUNT(*) AS contracts_count,
    SUM(contract_price) AS total_contract_value
FROM contracts
GROUP BY purchase_method
ORDER BY total_contract_value DESC;


-- 5. Динамика контрактов по месяцам
SELECT
    DATE_TRUNC('month', contract_date) AS contract_month,
    COUNT(*) AS contracts_count,
    SUM(contract_price) AS total_contract_value
FROM contracts
GROUP BY DATE_TRUNC('month', contract_date)
ORDER BY contract_month;


-- 6. Самые дорогие контракты
SELECT
    registry_id,
    customer_name,
    supplier_name,
    contract_subject,
    contract_price,
    contract_date
FROM contracts
ORDER BY contract_price DESC
LIMIT 10;


-- 7. Топ-15 объектов закупки по сумме
SELECT
    item_name,
    SUM(item_amount) AS total_item_amount
FROM contract_items
GROUP BY item_name
ORDER BY total_item_amount DESC
LIMIT 15;


-- 8. Топ-15 объектов закупки по частоте
SELECT
    item_name,
    COUNT(*) AS item_rows_count
FROM contract_items
GROUP BY item_name
ORDER BY item_rows_count DESC
LIMIT 15;