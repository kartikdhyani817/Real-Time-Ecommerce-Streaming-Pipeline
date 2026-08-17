-- ============================================
-- Real-Time E-Commerce Analytics
-- ============================================


-- Overall KPIs

SELECT
    COUNT(*) AS total_orders,
    SUM(quantity) AS total_items,
    ROUND(SUM(total_amount), 2)
        AS total_revenue,
    ROUND(AVG(total_amount), 2)
        AS average_order_value
FROM ecommerce_events;


-- ============================================
-- Revenue by Category
-- ============================================

SELECT
    category,
    COUNT(*) AS orders,
    SUM(quantity) AS items_sold,
    ROUND(SUM(total_amount), 2)
        AS revenue
FROM ecommerce_events
GROUP BY category
ORDER BY revenue DESC;


-- ============================================
-- Top Products
-- ============================================

SELECT
    product_name,
    COUNT(*) AS orders,
    SUM(quantity) AS quantity_sold,
    ROUND(SUM(total_amount), 2)
        AS revenue
FROM ecommerce_events
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 5;


-- ============================================
-- Payment Method Analysis
-- ============================================

SELECT
    payment_method,
    COUNT(*) AS orders,
    ROUND(SUM(total_amount), 2)
        AS revenue
FROM ecommerce_events
GROUP BY payment_method
ORDER BY orders DESC;


-- ============================================
-- Order Status Analysis
-- ============================================

SELECT
    order_status,
    COUNT(*) AS orders,
    ROUND(SUM(total_amount), 2)
        AS order_value
FROM ecommerce_events
GROUP BY order_status
ORDER BY orders DESC;


-- ============================================
-- Top Cities
-- ============================================

SELECT
    city,
    country,
    COUNT(*) AS orders,
    ROUND(SUM(total_amount), 2)
        AS revenue
FROM ecommerce_events
GROUP BY
    city,
    country
ORDER BY revenue DESC
LIMIT 10;