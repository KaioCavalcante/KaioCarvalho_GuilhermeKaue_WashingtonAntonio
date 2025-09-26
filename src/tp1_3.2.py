import argparse
import psycopg
import sys
from datetime import datetime
import re

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS product_group (
    group_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS product (
    asin TEXT PRIMARY KEY,
    title TEXT,
    salesrank INT,
    group_id INT NOT NULL REFERENCES product_group(group_id)
);

CREATE TABLE IF NOT EXISTS customer (
    customer_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS category (
    category_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS product_category (
    asin TEXT NOT NULL REFERENCES product(asin) ON DELETE CASCADE,
    category_id INT NOT NULL REFERENCES category(category_id) ON DELETE CASCADE,
    PRIMARY KEY (asin, category_id)
);

CREATE TABLE IF NOT EXISTS product_similar (
    asin TEXT NOT NULL,
    similar_asin TEXT NOT NULL,
    PRIMARY KEY (asin, similar_asin),
    FOREIGN KEY (asin) REFERENCES product(asin) ON DELETE CASCADE,
    FOREIGN KEY (similar_asin) REFERENCES product(asin) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS review (
    asin TEXT NOT NULL REFERENCES product(asin) ON DELETE CASCADE,
    customer_id TEXT NOT NULL REFERENCES customer(customer_id) ON DELETE CASCADE,
    review_date DATE NOT NULL,
    rating INT CHECK (rating BETWEEN 1 AND 5),
    votes INT DEFAULT 0,
    helpful INT DEFAULT 0,
    PRIMARY KEY (asin, customer_id, review_date)
);
"""

REVIEW_REGEX = re.compile(
    r"(\d{4}-\d{1,2}-\d{1,2})\s+(?:cutomer|customer):\s*(\S+)\s+rating:\s*(\d+)\s+votes:\s*(\d+)\s+helpful:\s*(\d+)",
    re.IGNORECASE
)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", required=True, type=int)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--input", required=True)
    return parser.parse_args()

def process_file(file_path):
    products, similars, categories, reviews = [], [], [], []
    customers = set()
    current_product = {}

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip()

            if line.startswith("ASIN:"):
                current_product["asin"] = line.split("ASIN:")[1].strip()

            elif line.startswith("  title:"):
                current_product["title"] = line.split("title:")[1].strip()

            elif line.startswith("  salesrank:"):
                try:
                    current_product["salesrank"] = int(line.split("salesrank:")[1].strip())
                except:
                    current_product["salesrank"] = None

            elif line.startswith("  group:"):
                current_product["group"] = line.split("group:")[1].strip()

            elif line.startswith("  similar:"):
                parts = line.split()
                main_asin = current_product.get("asin")
                if main_asin and len(parts) > 2:
                    for sim_asin in parts[2:]:
                        similars.append((main_asin, sim_asin))

            elif line.lstrip().startswith("|"):  
                main_asin = current_product.get("asin")
                if main_asin:
                    parts = [p for p in line.strip().split("|") if p]
                    for cat in parts:
                        categories.append((main_asin, cat))

            elif line.startswith("    "):  
                match = REVIEW_REGEX.search(line)
                if match:
                    try:
                        review_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
                        customer_id = match.group(2)
                        rating = int(match.group(3))
                        votes = int(match.group(4))
                        helpful = int(match.group(5))
                        asin = current_product.get("asin")
                        if asin:
                            reviews.append((asin, customer_id, review_date, rating, votes, helpful))
                            customers.add(customer_id)
                    except:
                        continue

            elif line == "": 
                if "asin" in current_product:
                    products.append((
                        current_product.get("asin"),
                        current_product.get("title"),
                        current_product.get("salesrank"),
                        current_product.get("group")
                    ))
                current_product = {}

    return products, similars, categories, list(customers), reviews

def insert_into_db(products, similars, categories, customers, reviews, conn):
    with conn.cursor() as cur:
        print("[INFO] Criando schema...")
        cur.execute(SCHEMA_SQL)
        conn.commit()

        groups = set([p[3] for p in products if p[3] is not None])
        groups.add("Unknown")
        group_map = {}

        for g in groups:
            cur.execute(
                "INSERT INTO product_group (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING group_id;",
                (g,)
            )
            res = cur.fetchone()
            if res:
                group_map[g] = res[0]

        cur.execute("SELECT group_id, name FROM product_group;")
        for gid, name in cur.fetchall():
            group_map[name] = gid

        product_values = [
            (asin, title, salesrank, group_map.get(group, group_map["Unknown"]))
            for asin, title, salesrank, group in products
        ]
        cur.executemany(
            "INSERT INTO product (asin, title, salesrank, group_id) VALUES (%s,%s,%s,%s) ON CONFLICT (asin) DO NOTHING;",
            product_values
        )
        conn.commit()
        print(f"[INFO] {len(product_values)} produtos inseridos")

        cur.executemany(
            "INSERT INTO customer (customer_id) VALUES (%s) ON CONFLICT (customer_id) DO NOTHING;",
            [(c,) for c in customers]
        )
        conn.commit()
        print(f"[INFO] {len(customers)} clientes inseridos")

        category_set = set([c[1] for c in categories])
        category_map = {}
        for cat in category_set:
            cur.execute(
                "INSERT INTO category (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING category_id;",
                (cat,)
            )
            res = cur.fetchone()
            if res:
                category_map[cat] = res[0]

        cur.execute("SELECT category_id, name FROM category;")
        for cid, name in cur.fetchall():
            category_map[name] = cid

        product_category_values = [
            (asin, category_map.get(cat))
            for asin, cat in categories
            if cat in category_map
        ]
        cur.executemany(
            "INSERT INTO product_category (asin, category_id) VALUES (%s,%s) ON CONFLICT DO NOTHING;",
            product_category_values
        )
        conn.commit()
        print(f"[INFO] {len(product_category_values)} relações produto-categoria inseridas")

        valid_asins = set([p[0] for p in products])
        similars_validos = [(a, s) for a, s in similars if s in valid_asins and a in valid_asins]
        cur.executemany(
            "INSERT INTO product_similar (asin, similar_asin) VALUES (%s,%s) ON CONFLICT DO NOTHING;",
            similars_validos
        )
        conn.commit()
        print(f"[INFO] {len(similars_validos)} relações produto-similar inseridas")

        cur.executemany(
            "INSERT INTO review (asin, customer_id, review_date, rating, votes, helpful) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;",
            reviews
        )
        conn.commit()
        print(f"[INFO] {len(reviews)} reviews inseridas")

def main():
    args = parse_args()
    print(f"[INFO] Lendo arquivo: {args.input}")

    try:
        products, similars, categories, customers, reviews = process_file(args.input)
        print(f"[INFO] Produtos: {len(products)}, Similares: {len(similars)}, Categorias: {len(categories)}, Clientes: {len(customers)}, Reviews: {len(reviews)}")
    except Exception as e:
        print(f"[ERRO] Falha ao processar arquivo: {e}")
        sys.exit(1)

    try:
        print("[INFO] Conectando ao banco de dados...")
        with psycopg.connect(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_pass
        ) as conn:
            print("[INFO] Inserindo dados no banco...")
            insert_into_db(products, similars, categories, customers, reviews, conn)
    except Exception as e:
        print(f"[ERRO] Falha ao inserir no banco: {e}")
        sys.exit(2)

    print("[INFO] Concluído com sucesso!")
    sys.exit(0)

if __name__ == "__main__":
    main()
