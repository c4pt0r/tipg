-- ============================================================================
-- pg-tikv Fancy SQL Demo
-- A comprehensive showcase of pg-tikv's PostgreSQL compatibility
-- ============================================================================

-- ============================================================================
-- PART 1: E-Commerce Analytics Platform
-- ============================================================================

-- Setup: Create a mini e-commerce schema
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS categories CASCADE;

-- Categories with hierarchical structure
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    parent_id INTEGER REFERENCES categories(id),
    metadata JSONB DEFAULT '{}'
);

-- Products catalog
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category_id INTEGER REFERENCES categories(id),
    price DOUBLE PRECISION NOT NULL,
    stock INTEGER DEFAULT 0,
    attributes JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Customers with rich profile
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    tier TEXT DEFAULT 'bronze' CHECK (tier IN ('bronze', 'silver', 'gold', 'platinum')),
    profile JSONB DEFAULT '{}',
    joined_at TIMESTAMP DEFAULT NOW()
);

-- Orders
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')),
    total DOUBLE PRECISION DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP
);

-- Order line items
CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DOUBLE PRECISION NOT NULL,
    discount DOUBLE PRECISION DEFAULT 0
);

-- Create indexes for performance
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_products_category ON products(category_id);

-- ============================================================================
-- PART 2: Seed Data
-- ============================================================================

-- Insert categories (hierarchical)
INSERT INTO categories (name, parent_id, metadata) VALUES
    ('Electronics', NULL, '{"icon": "laptop", "featured": true}'),
    ('Phones', 1, '{"icon": "phone"}'),
    ('Laptops', 1, '{"icon": "computer"}'),
    ('Accessories', 1, '{"icon": "headphones"}'),
    ('Clothing', NULL, '{"icon": "shirt", "featured": true}'),
    ('Men', 5, '{}'),
    ('Women', 5, '{}'),
    ('Home & Garden', NULL, '{"icon": "home"}');

-- Insert products
INSERT INTO products (name, category_id, price, stock, attributes) VALUES
    ('iPhone 15 Pro', 2, 999.99, 50, '{"color": "titanium", "storage": "256GB", "rating": 4.8}'),
    ('Samsung Galaxy S24', 2, 899.99, 75, '{"color": "black", "storage": "128GB", "rating": 4.6}'),
    ('MacBook Pro 14"', 3, 1999.99, 30, '{"chip": "M3 Pro", "ram": "18GB", "rating": 4.9}'),
    ('Dell XPS 15', 3, 1499.99, 25, '{"processor": "i7", "ram": "16GB", "rating": 4.5}'),
    ('AirPods Pro', 4, 249.99, 200, '{"generation": "2nd", "rating": 4.7}'),
    ('USB-C Hub', 4, 59.99, 500, '{"ports": 7, "rating": 4.3}'),
    ('Classic T-Shirt', 6, 29.99, 1000, '{"sizes": ["S","M","L","XL"], "material": "cotton"}'),
    ('Denim Jeans', 6, 79.99, 300, '{"sizes": ["30","32","34","36"], "fit": "slim"}'),
    ('Summer Dress', 7, 89.99, 150, '{"sizes": ["XS","S","M","L"], "pattern": "floral"}'),
    ('Smart Lamp', 8, 49.99, 400, '{"smart": true, "watts": 10, "rating": 4.4}');

-- Insert customers
INSERT INTO customers (email, name, tier, profile) VALUES
    ('alice@example.com', 'Alice Johnson', 'platinum', '{"preferences": {"newsletter": true}, "location": "NYC"}'),
    ('bob@example.com', 'Bob Smith', 'gold', '{"preferences": {"newsletter": false}, "location": "LA"}'),
    ('carol@example.com', 'Carol White', 'silver', '{"preferences": {"newsletter": true}, "location": "Chicago"}'),
    ('david@example.com', 'David Brown', 'bronze', '{"location": "Miami"}'),
    ('eve@example.com', 'Eve Davis', 'gold', '{"preferences": {"newsletter": true}, "location": "Seattle"}');

-- Insert orders with various statuses
INSERT INTO orders (customer_id, status, total, created_at, shipped_at, delivered_at) VALUES
    (1, 'delivered', 1249.98, NOW() - INTERVAL '30 days', NOW() - INTERVAL '28 days', NOW() - INTERVAL '25 days'),
    (1, 'delivered', 2059.98, NOW() - INTERVAL '15 days', NOW() - INTERVAL '13 days', NOW() - INTERVAL '10 days'),
    (2, 'shipped', 929.98, NOW() - INTERVAL '5 days', NOW() - INTERVAL '3 days', NULL),
    (2, 'pending', 159.98, NOW() - INTERVAL '1 day', NULL, NULL),
    (3, 'delivered', 109.98, NOW() - INTERVAL '20 days', NOW() - INTERVAL '18 days', NOW() - INTERVAL '15 days'),
    (3, 'confirmed', 1999.99, NOW() - INTERVAL '2 days', NULL, NULL),
    (4, 'cancelled', 249.99, NOW() - INTERVAL '10 days', NULL, NULL),
    (5, 'delivered', 339.97, NOW() - INTERVAL '25 days', NOW() - INTERVAL '23 days', NOW() - INTERVAL '20 days'),
    (5, 'shipped', 79.99, NOW() - INTERVAL '4 days', NOW() - INTERVAL '2 days', NULL),
    (1, 'pending', 899.99, NOW(), NULL, NULL);

-- Insert order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount) VALUES
    (1, 1, 1, 999.99, 0), (1, 5, 1, 249.99, 0),
    (2, 3, 1, 1999.99, 0), (2, 6, 1, 59.99, 0),
    (3, 2, 1, 899.99, 0), (3, 7, 1, 29.99, 0),
    (4, 7, 2, 29.99, 0), (4, 10, 2, 49.99, 0),
    (5, 7, 1, 29.99, 0), (5, 8, 1, 79.99, 0),
    (6, 3, 1, 1999.99, 0),
    (7, 5, 1, 249.99, 0),
    (8, 5, 1, 249.99, 0), (8, 9, 1, 89.98, 10.00),
    (9, 8, 1, 79.99, 0),
    (10, 2, 1, 899.99, 0);

-- ============================================================================
-- PART 3: Fancy Queries
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 3.1 Window Functions: Customer Purchase Rankings
-- ---------------------------------------------------------------------------
SELECT 
    '=== Customer Purchase Rankings ===' as section;

SELECT 
    c.name as customer,
    c.tier,
    COUNT(o.id) as order_count,
    COALESCE(SUM(o.total), 0) as total_spent,
    RANK() OVER (ORDER BY COALESCE(SUM(o.total), 0) DESC) as spending_rank,
    ROUND(
        100.0 * COALESCE(SUM(o.total), 0) / 
        SUM(COALESCE(SUM(o.total), 0)) OVER (), 
        2
    ) as pct_of_revenue
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id AND o.status != 'cancelled'
GROUP BY c.id, c.name, c.tier
ORDER BY total_spent DESC;

-- ---------------------------------------------------------------------------
-- 3.2 Recursive CTE: Category Hierarchy with Product Counts
-- ---------------------------------------------------------------------------
SELECT 
    '=== Category Hierarchy ===' as section;

WITH RECURSIVE category_tree AS (
    -- Base case: top-level categories
    SELECT 
        id, 
        name, 
        parent_id, 
        0 as depth,
        name as path
    FROM categories 
    WHERE parent_id IS NULL
    
    UNION ALL
    
    -- Recursive case: child categories
    SELECT 
        c.id, 
        c.name, 
        c.parent_id, 
        ct.depth + 1,
        ct.path || ' > ' || c.name
    FROM categories c
    JOIN category_tree ct ON c.parent_id = ct.id
)
SELECT 
    REPEAT('  ', depth) || name as category,
    path,
    (SELECT COUNT(*) FROM products WHERE category_id = category_tree.id) as product_count
FROM category_tree
ORDER BY path;

-- ---------------------------------------------------------------------------
-- 3.3 Complex CTE: Monthly Sales Dashboard
-- ---------------------------------------------------------------------------
SELECT 
    '=== Monthly Sales Dashboard ===' as section;

WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', o.created_at) as month,
        COUNT(DISTINCT o.id) as orders,
        COUNT(DISTINCT o.customer_id) as unique_customers,
        SUM(o.total) as revenue
    FROM orders o
    WHERE o.status NOT IN ('cancelled', 'pending')
    GROUP BY DATE_TRUNC('month', o.created_at)
),
monthly_growth AS (
    SELECT 
        month,
        orders,
        unique_customers,
        revenue,
        LAG(revenue) OVER (ORDER BY month) as prev_revenue,
        revenue - LAG(revenue) OVER (ORDER BY month) as revenue_change
    FROM monthly_sales
)
SELECT 
    TO_CHAR(month, 'YYYY-MM') as month,
    orders,
    unique_customers,
    ROUND(revenue::numeric, 2) as revenue,
    CASE 
        WHEN prev_revenue IS NULL THEN 'N/A'
        WHEN prev_revenue = 0 THEN '+∞%'
        ELSE ROUND(((revenue - prev_revenue) / prev_revenue * 100)::numeric, 1) || '%'
    END as growth
FROM monthly_growth
ORDER BY month;

-- ---------------------------------------------------------------------------
-- 3.4 JSONB Queries: Product Attribute Analysis
-- ---------------------------------------------------------------------------
SELECT 
    '=== Products with High Ratings ===' as section;

SELECT 
    p.name,
    c.name as category,
    p.price,
    p.attributes->>'rating' as rating,
    p.attributes->>'color' as color,
    CASE 
        WHEN (p.attributes->>'rating')::float >= 4.7 THEN '⭐⭐⭐ Premium'
        WHEN (p.attributes->>'rating')::float >= 4.5 THEN '⭐⭐ Quality'
        ELSE '⭐ Standard'
    END as tier
FROM products p
JOIN categories c ON p.category_id = c.id
WHERE p.attributes ? 'rating'
ORDER BY (p.attributes->>'rating')::float DESC;

-- ---------------------------------------------------------------------------
-- 3.5 Subquery Magic: Best Sellers by Category
-- ---------------------------------------------------------------------------
SELECT 
    '=== Best Seller per Category ===' as section;

SELECT 
    c.name as category,
    p.name as best_seller,
    stats.units_sold,
    stats.revenue
FROM categories c
JOIN LATERAL (
    SELECT 
        oi.product_id,
        SUM(oi.quantity) as units_sold,
        SUM(oi.quantity * oi.unit_price - oi.discount) as revenue
    FROM order_items oi
    JOIN products prod ON oi.product_id = prod.id
    WHERE prod.category_id = c.id
    GROUP BY oi.product_id
    ORDER BY SUM(oi.quantity) DESC
    LIMIT 1
) stats ON true
JOIN products p ON stats.product_id = p.id
ORDER BY stats.revenue DESC;

-- ---------------------------------------------------------------------------
-- 3.6 Window Functions: Running Totals & Moving Averages
-- ---------------------------------------------------------------------------
SELECT 
    '=== Order Timeline Analysis ===' as section;

SELECT 
    o.id as order_id,
    c.name as customer,
    o.created_at::date as order_date,
    o.total,
    SUM(o.total) OVER (ORDER BY o.created_at) as running_total,
    ROUND(
        AVG(o.total) OVER (
            ORDER BY o.created_at 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::numeric, 
        2
    ) as moving_avg_3
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.status != 'cancelled'
ORDER BY o.created_at;

-- ---------------------------------------------------------------------------
-- 3.7 Advanced Aggregation: Customer Cohort Analysis
-- ---------------------------------------------------------------------------
SELECT 
    '=== Customer Cohort Analysis ===' as section;

WITH customer_cohorts AS (
    SELECT 
        c.id,
        c.name,
        DATE_TRUNC('month', c.joined_at) as cohort_month,
        MIN(o.created_at) as first_order,
        MAX(o.created_at) as last_order,
        COUNT(o.id) as order_count,
        COALESCE(SUM(o.total), 0) as lifetime_value
    FROM customers c
    LEFT JOIN orders o ON c.id = o.customer_id AND o.status != 'cancelled'
    GROUP BY c.id, c.name, DATE_TRUNC('month', c.joined_at)
)
SELECT 
    name,
    TO_CHAR(cohort_month, 'YYYY-MM') as joined,
    order_count as orders,
    ROUND(lifetime_value::numeric, 2) as ltv,
    CASE 
        WHEN last_order > NOW() - INTERVAL '7 days' THEN '🟢 Active'
        WHEN last_order > NOW() - INTERVAL '30 days' THEN '🟡 Recent'
        WHEN last_order IS NOT NULL THEN '🔴 Churned'
        ELSE '⚪ Never Ordered'
    END as status
FROM customer_cohorts
ORDER BY lifetime_value DESC;

-- ---------------------------------------------------------------------------
-- 3.8 DISTINCT ON: Latest Order per Customer
-- ---------------------------------------------------------------------------
SELECT 
    '=== Latest Order per Customer ===' as section;

SELECT DISTINCT ON (o.customer_id)
    c.name as customer,
    o.id as order_id,
    o.status,
    o.total,
    o.created_at::date as order_date
FROM orders o
JOIN customers c ON o.customer_id = c.id
ORDER BY o.customer_id, o.created_at DESC;

-- ---------------------------------------------------------------------------
-- 3.9 Complex CASE & String Functions
-- ---------------------------------------------------------------------------
SELECT 
    '=== Customer Tier Recommendations ===' as section;

WITH customer_metrics AS (
    SELECT 
        c.id,
        c.name,
        c.tier as current_tier,
        COUNT(o.id) as orders,
        COALESCE(SUM(o.total), 0) as total_spent,
        COALESCE(AVG(o.total), 0) as avg_order
    FROM customers c
    LEFT JOIN orders o ON c.id = o.customer_id AND o.status != 'cancelled'
    GROUP BY c.id, c.name, c.tier
)
SELECT 
    UPPER(SUBSTRING(name, 1, 1)) || LOWER(SUBSTRING(name, 2)) as customer,
    INITCAP(current_tier) as current,
    CASE 
        WHEN total_spent >= 3000 THEN 'Platinum'
        WHEN total_spent >= 1500 THEN 'Gold'
        WHEN total_spent >= 500 THEN 'Silver'
        ELSE 'Bronze'
    END as recommended_tier,
    CASE 
        WHEN total_spent >= 3000 AND current_tier != 'platinum' THEN '⬆️ Upgrade!'
        WHEN total_spent < 500 AND current_tier = 'platinum' THEN '⬇️ Review'
        ELSE '✓ OK'
    END as action,
    ROUND(total_spent::numeric, 2) as total_spent
FROM customer_metrics
ORDER BY total_spent DESC;

-- ---------------------------------------------------------------------------
-- 3.10 Grand Finale: Executive Summary View
-- ---------------------------------------------------------------------------
SELECT 
    '=== Executive Summary ===' as section;

CREATE VIEW executive_summary AS
WITH metrics AS (
    SELECT 
        COUNT(DISTINCT c.id) as total_customers,
        COUNT(DISTINCT CASE WHEN o.created_at > NOW() - INTERVAL '30 days' THEN c.id END) as active_customers,
        COUNT(o.id) as total_orders,
        COUNT(CASE WHEN o.status = 'delivered' THEN 1 END) as delivered_orders,
        SUM(CASE WHEN o.status != 'cancelled' THEN o.total ELSE 0 END) as total_revenue,
        AVG(CASE WHEN o.status != 'cancelled' THEN o.total END) as avg_order_value
    FROM customers c
    LEFT JOIN orders o ON c.id = o.customer_id
),
top_product AS (
    SELECT p.name, SUM(oi.quantity) as sold
    FROM order_items oi
    JOIN products p ON oi.product_id = p.id
    GROUP BY p.name
    ORDER BY sold DESC
    LIMIT 1
),
top_category AS (
    SELECT c.name, SUM(oi.quantity * oi.unit_price) as revenue
    FROM order_items oi
    JOIN products p ON oi.product_id = p.id
    JOIN categories c ON p.category_id = c.id
    GROUP BY c.name
    ORDER BY revenue DESC
    LIMIT 1
)
SELECT 
    m.total_customers as "Total Customers",
    m.active_customers as "Active (30d)",
    m.total_orders as "Total Orders",
    m.delivered_orders as "Delivered",
    ROUND(m.total_revenue::numeric, 2) as "Revenue",
    ROUND(m.avg_order_value::numeric, 2) as "Avg Order",
    tp.name as "Top Product",
    tc.name as "Top Category"
FROM metrics m, top_product tp, top_category tc;

SELECT * FROM executive_summary;

-- ============================================================================
-- PART 4: Cleanup (Optional)
-- ============================================================================
-- Uncomment to clean up:
-- DROP VIEW IF EXISTS executive_summary;
-- DROP TABLE IF EXISTS order_items CASCADE;
-- DROP TABLE IF EXISTS orders CASCADE;
-- DROP TABLE IF EXISTS products CASCADE;
-- DROP TABLE IF EXISTS customers CASCADE;
-- DROP TABLE IF EXISTS categories CASCADE;

SELECT '✅ Demo completed successfully!' as result;
