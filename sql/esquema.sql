DROP TABLE IF EXISTS review CASCADE;
DROP TABLE IF EXISTS product_similar CASCADE;
DROP TABLE IF EXISTS product_category CASCADE;
DROP TABLE IF EXISTS category CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS product CASCADE;
DROP TABLE IF EXISTS product_group CASCADE;

CREATE TABLE product_group (
    group_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE product (
    asin TEXT PRIMARY KEY,
    title TEXT,
    salesrank INTEGER,
    group_id INT REFERENCES product_group(group_id)
);

CREATE TABLE customer (
    customer_id TEXT PRIMARY KEY
);

CREATE TABLE category (
    category_id INT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE product_category (
    asin TEXT REFERENCES product(asin),
    category_id INT REFERENCES category(category_id),
    PRIMARY KEY (asin, category_id)
);

CREATE TABLE product_similar (
    asin        VARCHAR(20) NOT NULL,
    similar_asin VARCHAR(20) NOT NULL,
    PRIMARY KEY (asin, similar_asin),
    FOREIGN KEY (asin) REFERENCES product(asin),
    FOREIGN KEY (similar_asin) REFERENCES product(asin)
);

CREATE TABLE review (
    asin TEXT REFERENCES product(asin),
    customer_id TEXT REFERENCES customer(customer_id),
    review_date DATE NOT NULL,
    rating INT CHECK (rating BETWEEN 1 AND 5),
    votes INT,
    helpful INT,
    PRIMARY KEY (asin, customer_id, review_date)
);
