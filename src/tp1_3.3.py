import argparse
import sys
import pandas as pd
from db import DB


def log(msg):
    print(f"[DASHBOARD] {msg}")


def run(args):
    db = DB(args.db_host, args.db_port, args.db_name, args.db_user, args.db_pass)

    with db.connect() as conn:
        def execute_query(title, sql, params=None):
            log(title)
            cur = conn.cursor()
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            if not rows:
                print("Nenhum resultado encontrado.\n")
            else:
                df = pd.DataFrame(rows, columns=columns)
                print(df.to_string(index=False))
                print("-" * 80)
            return rows

        # 1. Reviews mais úteis (positivas e negativas)
        if args.product_asin:
            execute_query(
                "Top 5 reviews positivas",
                """
                SELECT r.customer_id, r.rating, r.votes, r.helpful, r.review_date
                FROM review r
                WHERE r.asin = %s
                ORDER BY r.rating DESC, r.helpful DESC
                LIMIT 5
                """,
                params=(args.product_asin,)
            )
            execute_query(
                "Top 5 reviews negativas",
                """
                SELECT r.customer_id, r.rating, r.votes, r.helpful, r.review_date
                FROM review r
                WHERE r.asin = %s
                ORDER BY r.rating ASC, r.helpful DESC
                LIMIT 5
                """,
                params=(args.product_asin,)
            )

        # 2. Produtos similares com melhor salesrank
        if args.product_asin:
            execute_query(
                "Produtos similares com melhor salesrank",
                """
                SELECT ps.similar_asin, p.title, p.salesrank
                FROM product_similar ps
                JOIN product p ON ps.similar_asin = p.asin
                WHERE ps.asin = %s AND p.salesrank IS NOT NULL
                ORDER BY p.salesrank ASC
                """,
                params=(args.product_asin,)
            )

        # 3. Evolução diária da média de avaliações
        if args.product_asin:
            execute_query(
                "Evolução diária das médias de avaliação",
                """
                SELECT r.review_date, AVG(r.rating) AS avg_rating
                FROM review r
                WHERE r.asin = %s
                GROUP BY r.review_date
                ORDER BY r.review_date
                """,
                params=(args.product_asin,)
            )

        # 4. Top 10 produtos líderes de venda por grupo
        execute_query(
            "Top 10 produtos líderes de venda por grupo",
            """
            WITH ranked AS (
                SELECT g.name AS group_name, p.asin, p.title, p.salesrank,
                       ROW_NUMBER() OVER (PARTITION BY g.name ORDER BY p.salesrank ASC) AS rn
                FROM product p
                JOIN product_group g ON p.group_id = g.group_id
                WHERE p.salesrank IS NOT NULL
            )
            SELECT group_name, asin, title, salesrank
            FROM ranked
            WHERE rn <= 10
            """
        )

        # 5. Top 10 produtos com maior média de avaliações úteis positivas
        execute_query(
            "Top 10 produtos com maior média de avaliações úteis positivas",
            """
            SELECT p.asin, p.title,
                   AVG(CASE WHEN r.votes > 0 THEN r.helpful::float / r.votes ELSE 0 END) AS avg_usefulness
            FROM product p
            JOIN review r ON p.asin = r.asin
            GROUP BY p.asin, p.title
            ORDER BY avg_usefulness DESC
            LIMIT 10
            """
        )

        # 6. Top 5 categorias com maior média de avaliações úteis positivas
        execute_query(
            "Top 5 categorias com maior média de avaliações úteis positivas por produto",
            """
            SELECT c.name,
                   AVG(CASE WHEN r.votes > 0 THEN r.helpful::float / r.votes ELSE 0 END) AS avg_usefulness
            FROM category c
            JOIN product_category pc ON c.category_id = pc.category_id
            JOIN review r ON pc.asin = r.asin
            GROUP BY c.name
            ORDER BY avg_usefulness DESC
            LIMIT 5
            """
        )

        # 7. Top 10 clientes que mais comentaram por grupo
        execute_query(
            "Top 10 clientes que mais comentaram por grupo",
            """
            SELECT g.name AS group_name, r.customer_id, COUNT(*) AS total_reviews
            FROM review r
            JOIN product p ON r.asin = p.asin
            JOIN product_group g ON p.group_id = g.group_id
            GROUP BY g.name, r.customer_id
            ORDER BY g.name, total_reviews DESC
            """
        )

    log("Consultas finalizadas com sucesso")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Dashboard de consultas SQL")
    parser.add_argument('--db-host', required=True)
    parser.add_argument('--db-port', type=int, required=True)
    parser.add_argument('--db-name', required=True)
    parser.add_argument('--db-user', required=True)
    parser.add_argument('--db-pass', required=True)
    parser.add_argument('--product-asin', help="ASIN do produto para consultas específicas")
    args = parser.parse_args()

    try:
        run(args)
        sys.exit(0)
    except Exception as e:
        print(f"[ERRO] {e}", file=sys.stderr)
        sys.exit(1)
