DROP TABLE IF EXISTS product_categories CASCADE;
DROP TABLE IF EXISTS product_similar CASCADE;
DROP TABLE IF EXISTS reviews CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS products CASCADE;


CREATE TABLE products (
    asin CHAR(10) PRIMARY KEY,
    product_id INTEGER,
    title VARCHAR(100),
    group_name VARCHAR(100),
    salesrank INTEGER
);


CREATE TABLE product_similar (
    asin CHAR(10) NOT NULL,
    similar_asin CHAR(10) NOT NULL,
    PRIMARY KEY (asin, similar_asin),
    FOREIGN KEY (asin) REFERENCES products(asin) ON DELETE CASCADE,
    FOREIGN KEY (similar_asin) REFERENCES products(asin) ON DELETE CASCADE
);


CREATE TABLE categories (
    category_id INTEGER PRIMARY KEY,
    path VARCHAR(100)
);

CREATE TABLE product_categories (
    asin CHAR(10) NOT NULL,
    category_id INTEGER NOT NULL,
    PRIMARY KEY (asin, category_id),
    FOREIGN KEY (asin) REFERENCES products(asin) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES categories(category_id) ON DELETE CASCADE
);


CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY
);

CREATE TABLE reviews (
    review_id SERIAL PRIMARY KEY,
    asin CHAR(10) NOT NULL,
    customer_id TEXT NOT NULL,
    review_date DATE NOT NULL,
    rating INT CHECK (rating BETWEEN 1 AND 5),
    votes INT DEFAULT 0,
    helpful INT DEFAULT 0,
    FOREIGN KEY (asin) REFERENCES products(asin) ON DELETE CASCADE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
);
