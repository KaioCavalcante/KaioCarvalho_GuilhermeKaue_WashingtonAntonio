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
    group_id INT NOT NULL REFERENCES product_group(group_id)
);


CREATE TABLE customer (
    customer_id TEXT PRIMARY KEY
);


CREATE TABLE category (
    category_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE product_category (
    asin TEXT NOT NULL REFERENCES product(asin) ON DELETE CASCADE,
    category_id INT NOT NULL REFERENCES category(category_id) ON DELETE CASCADE,
    PRIMARY KEY (asin, category_id)
);

CREATE TABLE product_similar (
    asin TEXT NOT NULL,
    similar_asin TEXT NOT NULL,
    PRIMARY KEY (asin, similar_asin),
    FOREIGN KEY (asin) REFERENCES product(asin) ON DELETE CASCADE,
    FOREIGN KEY (similar_asin) REFERENCES product(asin) ON DELETE CASCADE
);

CREATE TABLE review (
    asin TEXT NOT NULL REFERENCES product(asin) ON DELETE CASCADE,
    customer_id TEXT NOT NULL REFERENCES customer(customer_id) ON DELETE CASCADE,
    review_date DATE NOT NULL,
    rating INT CHECK (rating BETWEEN 1 AND 5),
    votes INT DEFAULT 0,
    helpful INT DEFAULT 0,
    PRIMARY KEY (asin, customer_id, review_date)
);
