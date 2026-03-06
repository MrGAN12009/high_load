CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS products (
    id BIGSERIAL PRIMARY KEY,
    category_id INT NOT NULL REFERENCES categories(id) ON DELETE RESTRICT,
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    stock INT NOT NULL CHECK (stock >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_products_category_id ON products (category_id);
CREATE INDEX IF NOT EXISTS idx_products_name_lower ON products (LOWER(name));

INSERT INTO categories (name)
VALUES
    ('Electronics'),
    ('Home'),
    ('Fashion'),
    ('Beauty'),
    ('Sports'),
    ('Books'),
    ('Toys'),
    ('Automotive'),
    ('Food'),
    ('Pets')
ON CONFLICT (name) DO NOTHING;

WITH needed_rows AS (
    SELECT GREATEST(0, 1000 - COUNT(*)) AS cnt
    FROM products
),
series AS (
    SELECT generate_series(1, (SELECT cnt FROM needed_rows)) AS n
),
generated AS (
    SELECT
        n,
        (ARRAY[
            'Smart', 'Ultra', 'Eco', 'Classic', 'Pro', 'Compact', 'Premium', 'Daily', 'Urban', 'Flex'
        ])[1 + FLOOR(RANDOM() * 10)::INT] AS adjective,
        (ARRAY[
            'Portable', 'Modern', 'Advanced', 'Comfort', 'Essential', 'Travel', 'Performance', 'Studio', 'Family', 'Lite'
        ])[1 + FLOOR(RANDOM() * 10)::INT] AS style,
        (ARRAY[
            'Bundle', 'Set', 'Kit', 'Pack', 'Edition', 'Model', 'Series', 'Selection', 'Version', 'Collection'
        ])[1 + FLOOR(RANDOM() * 10)::INT] AS noun,
        (ARRAY[
            'Electronics', 'Home', 'Fashion', 'Beauty', 'Sports', 'Books', 'Toys', 'Automotive', 'Food', 'Pets'
        ])[1 + FLOOR(RANDOM() * 10)::INT] AS category_name
    FROM series
)
INSERT INTO products (category_id, name, description, price, stock)
SELECT
    c.id,
    CONCAT(g.adjective, ' ', g.style, ' ', g.noun, ' #', g.n),
    CONCAT('Marketplace product #', g.n, ' in category ', g.category_name, '.'),
    ROUND((5 + RANDOM() * 1995)::NUMERIC, 2),
    (10 + FLOOR(RANDOM() * 190))::INT
FROM generated g
JOIN categories c ON c.name = g.category_name;
