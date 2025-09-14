#!/usr/bin/env python3
import argparse
import sys
import time
from pathlib import Path
import io
from db import DB
from utils import parse_snap_lines

DDL_PATH = Path('/app/sql/esquema.sql')
BATCH_SIZE = 5000


def log(msg):
    print(f"[ETL] {msg}")


def run(args):
    start = time.time()
    db = DB(args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass)

    with db.connect() as conn:
        cur = conn.cursor()

        # Cria esquema
        log("Criando esquema")
        cur.execute(DDL_PATH.read_text())
        conn.commit()

        groups_seen = {}
        cur.execute("SELECT name, group_id FROM product_group")
        for name, gid in cur.fetchall():
            groups_seen[name] = gid

        def get_group_id(name: str):
            if not name:
                return None
            if name in groups_seen:
                return groups_seen[name]
            cur.execute(
                """
                INSERT INTO product_group(name) 
                VALUES (%s) 
                ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name 
                RETURNING group_id
                """,
                (name,)
            )
            gid = cur.fetchone()[0]
            groups_seen[name] = gid
            return gid

        # Buffers
        buf_product = io.StringIO()
        buf_customer = io.StringIO()
        buf_category = io.StringIO()
        buf_prod_cat = io.StringIO()
        buf_prod_sim = io.StringIO()
        buf_review = io.StringIO()

        customers_seen = set()
        categories_seen = set()
        products_seen = set()

        def flush_all():
            """Usa COPY em transação para desempenho"""
            if buf_product.getvalue():
                buf_product.seek(0)
                cur.copy_expert("COPY product(asin, title, salesrank, group_id) FROM STDIN WITH (FORMAT csv)", buf_product)
                buf_product.truncate(0)

            if buf_customer.getvalue():
                buf_customer.seek(0)
                cur.copy_expert("COPY customer(customer_id) FROM STDIN WITH (FORMAT csv)", buf_customer)
                buf_customer.truncate(0)

            if buf_category.getvalue():
                buf_category.seek(0)
                cur.copy_expert("COPY category(category_id, name) FROM STDIN WITH (FORMAT csv)", buf_category)
                buf_category.truncate(0)

            if buf_prod_cat.getvalue():
                buf_prod_cat.seek(0)
                cur.copy_expert("COPY product_category(asin, category_id) FROM STDIN WITH (FORMAT csv)", buf_prod_cat)
                buf_prod_cat.truncate(0)

            if buf_prod_sim.getvalue():
                buf_prod_sim.seek(0)
                cur.copy_expert("COPY product_similar(asin, similar_asin) FROM STDIN WITH (FORMAT csv)", buf_prod_sim)
                buf_prod_sim.truncate(0)

            if buf_review.getvalue():
                buf_review.seek(0)
                cur.copy_expert("COPY review(asin, customer_id, review_date, rating, votes, helpful) FROM STDIN WITH (FORMAT csv)", buf_review)
                buf_review.truncate(0)

            conn.commit()

        # Processa arquivo
        log(f"Lendo arquivo {args.input} ...")
        total_products = 0
        total_reviews = 0
        total_similars = 0
        total_categories = 0

        with open(args.input, 'r', encoding='utf-8') as f:
            for blk in parse_snap_lines(f):
                total_products += 1

                group_id = get_group_id(blk.group)

                asin = blk.asin if blk.asin else ""
                title = blk.title.replace(',', ' ') if blk.title else ""
                salesrank = blk.salesrank if blk.salesrank else ""

                buf_product.write(f"{asin},{title},{salesrank},{group_id}\n")
                products_seen.add(asin)

                # Categorias
                for path in blk.categories_paths:
                    for name, cid in path:
                        total_categories += 1
                        if cid not in categories_seen:
                            buf_category.write(f"{cid},{name}\n")
                            categories_seen.add(cid)
                        buf_prod_cat.write(f"{asin},{cid}\n")

                # Produtos similares
                for sim_asin in blk.similars:
                    if sim_asin:
                        total_similars += 1
                        buf_prod_sim.write(f"{asin},{sim_asin}\n")

                # Reviews
                for d, cust, rating, votes, helpful in blk.reviews:
                    total_reviews += 1
                    cust = cust if cust else "unknown"
                    d = d if d else "2000-01-01"
                    rating = rating if rating else 0
                    votes = votes if votes else 0
                    helpful = helpful if helpful else 0

                    if cust not in customers_seen:
                        buf_customer.write(f"{cust}\n")
                        customers_seen.add(cust)

                    buf_review.write(f"{asin},{cust},{d},{rating},{votes},{helpful}\n")

                if total_products % BATCH_SIZE == 0:
                    flush_all()
                    log(f"Processados {total_products} produtos...")

        flush_all()
        log(f"ETL finalizado.")
        log(f"Produtos: {total_products}, Categorias: {total_categories}, Similares: {total_similars}, Reviews: {total_reviews}")
        log(f"Tempo total: {time.time() - start:.2f} segundos")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="ETL SNAP -> PostgreSQL")
    parser.add_argument('--db-host', required=True)
    parser.add_argument('--db-port', type=int, required=True)
    parser.add_argument('--db-name', required=True)
    parser.add_argument('--db-user', required=True)
    parser.add_argument('--db-pass', required=True)
    parser.add_argument('--input', required=True, help="Caminho para o arquivo SNAP")
    args = parser.parse_args()

    try:
        run(args)
        sys.exit(0)
    except Exception as e:
        print(f"[ERRO] {e}", file=sys.stderr)
        sys.exit(1)
