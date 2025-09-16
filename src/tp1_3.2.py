import argparse
import psycopg
import pandas as pd
import sys

SCHEMA_SQL = """
-- Tabelas principais
CREATE TABLE IF NOT EXISTS product_group (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS product (
    asin TEXT PRIMARY KEY,
    title TEXT,
    salesrank INT,
    group_id INT REFERENCES product_group(id)
);

CREATE TABLE IF NOT EXISTS product_similar (
    asin TEXT REFERENCES product(asin),
    similar_asin TEXT,
    PRIMARY KEY (asin, similar_asin)
);

CREATE TABLE IF NOT EXISTS category (
    id SERIAL PRIMARY KEY,
    asin TEXT REFERENCES product(asin),
    category TEXT
);
"""

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
    products = []
    similars = []
    categories = []
    current_product = {}

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
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
                for sim_asin in parts[2:]:
                    similars.append({"asin": main_asin, "similar_asin": sim_asin})
            elif line.startswith("  categories:"):
                num_cats = int(line.split()[1])
                current_product["num_categories"] = num_cats
            elif line.startswith("|"):
                main_asin = current_product.get("asin")
                categories.append({"asin": main_asin, "category": line})
            elif line == "":
                if "asin" in current_product:
                    if "title" not in current_product:
                        current_product["title"] = None
                    if "salesrank" not in current_product:
                        current_product["salesrank"] = None
                    if "group" not in current_product:
                        current_product["group"] = None
                    products.append(current_product)
                current_product = {}

    if "asin" in current_product:
        if "title" not in current_product:
            current_product["title"] = None
        if "salesrank" not in current_product:
            current_product["salesrank"] = None
        if "group" not in current_product:
            current_product["group"] = None
        products.append(current_product)

    df_products = pd.DataFrame(products)
    df_similars = pd.DataFrame(similars)
    df_categories = pd.DataFrame(categories)
    return df_products, df_similars, df_categories

def insert_into_db(df_products, df_similars, df_categories, conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        conn.commit()
        groups = df_products["group"].dropna().unique()
        group_map = {}
        for g in groups:
            cur.execute("INSERT INTO product_group (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id;", (g,))
            res = cur.fetchone()
            if res:
                group_map[g] = res[0]
        
        cur.execute("SELECT id, name FROM product_group;")
        for gid, name in cur.fetchall():
            group_map[name] = gid

       
        for _, row in df_products.iterrows():
            gid = group_map.get(row.get("group"))
            cur.execute(
                "INSERT INTO product (asin, title, salesrank, group_id) VALUES (%s, %s, %s, %s) ON CONFLICT (asin) DO NOTHING;",
                (row["asin"], row["title"], row["salesrank"], gid)
            )

        
        for _, row in df_similars.iterrows():
            cur.execute(
                "INSERT INTO product_similar (asin, similar_asin) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                (row["asin"], row["similar_asin"])
            )

        
        for _, row in df_categories.iterrows():
            cur.execute(
                "INSERT INTO category (asin, category) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                (row["asin"], row["category"])
            )

        conn.commit()

def main():
    args = parse_args()
    print(f"Lendo arquivo: {args.input}")

    try:
        df_products, df_similars, df_categories = process_file(args.input)
        print(f"Produtos: {len(df_products)}, Similares: {len(df_similars)}, Categorias: {len(df_categories)}")
    except Exception as e:
        print(f"Falha ao processar arquivo: {e}")
        sys.exit(1)

    try:
        print("Conectando ao banco de dados...")
        with psycopg.connect(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_pass
        ) as conn:
            print("Inserindo dados no banco...")
            insert_into_db(df_products, df_similars, df_categories, conn)
    except Exception as e:
        print(f"Falha ao inserir no banco: {e}")
        sys.exit(2)

    print("Concluído com sucesso!")
    sys.exit(0)

if __name__ == "__main__":
    main()
