import argparse
import psycopg
import sys


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", required=True, type=int)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--output", required=False)
    parser.add_argument("--product-asin", required=False)
    return parser.parse_args()


def print_table(colnames, rows):
    if not rows:
        print("Nenhum resultado encontrado.\n")
        return

    widths = [max(len(str(x)) for x in [col] + [row[i] for row in rows]) for i, col in enumerate(colnames)]
    header = " | ".join(col.ljust(widths[i]) for i, col in enumerate(colnames))
    print(header)
    print("-" * len(header))

    for row in rows:
        line = " | ".join(str(row[i]).ljust(widths[i]) for i in range(len(row)))
        print(line)

    print("-" * len(header))
    print()


def execute_query(cur, title, query, params=None):
    print(f"[DASHBOARD] {title}")
    print("=" * 80)
    cur.execute(query, params or ())
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    print_table(colnames, rows)


def main():
    args = parse_args()

    try:
        with psycopg.connect(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_pass
        ) as conn:
            cur = conn.cursor()

            if args.product_asin:
                execute_query(cur,
                    "Top 5 comentários mais úteis (positivos) para o produto",
                    """
                    SELECT review_date, customer_id, rating, votes, helpful
                    FROM review
                    WHERE asin = %s
                    ORDER BY rating DESC, helpful DESC
                    LIMIT 5;
                    """,
                    (args.product_asin,)
                )

                execute_query(cur,
                    "Top 5 comentários mais úteis (negativos) para o produto",
                    """
                    SELECT review_date, customer_id, rating, votes, helpful
                    FROM review
                    WHERE asin = %s
                    ORDER BY rating ASC, helpful DESC
                    LIMIT 5;
                    """,
                    (args.product_asin,)
                )

                execute_query(cur,
                    "Produtos similares com melhor salesrank",
                    """
                    SELECT ps.similar_asin, p.title, p.salesrank
                    FROM product_similar ps
                    JOIN product p ON ps.similar_asin = p.asin
                    WHERE ps.asin = %s
                      AND p.salesrank < (SELECT salesrank FROM product WHERE asin = %s)
                    ORDER BY p.salesrank ASC
                    LIMIT 10;
                    """,
                    (args.product_asin, args.product_asin)
                )

                execute_query(cur,
                    "Evolução diária da média de avaliações do produto",
                    """
                    SELECT review_date, ROUND(AVG(rating)::numeric,2) as avg_rating
                    FROM review
                    WHERE asin = %s
                    GROUP BY review_date
                    ORDER BY review_date;
                    """,
                    (args.product_asin,)
                )

            execute_query(cur,
                "Top 10 produtos líderes de venda por grupo",
                """
                WITH ranked_products AS (
                    SELECT g.name as group_name, p.asin, p.title, p.salesrank,
                           ROW_NUMBER() OVER (PARTITION BY g.name ORDER BY p.salesrank ASC) AS rn
                    FROM product p
                    JOIN product_group g ON p.group_id = g.group_id
                    WHERE p.salesrank > 0
                )
                SELECT group_name, asin, title, salesrank
                FROM ranked_products
                WHERE rn <= 10
                ORDER BY group_name, salesrank;
                """
            )

            execute_query(cur,
                "Top 10 produtos com maior média de avaliações úteis positivas",
                """
                SELECT p.asin, p.title,
                       ROUND(AVG(CASE WHEN r.votes > 0 THEN r.helpful::decimal/r.votes ELSE 0 END), 2) as avg_usefulness
                FROM review r
                JOIN product p ON r.asin = p.asin
                GROUP BY p.asin, p.title
                ORDER BY avg_usefulness DESC
                LIMIT 10;
                """
            )

            execute_query(cur,
                "Top 5 categorias com maior média de avaliações úteis positivas",
                """
                SELECT c.name as category,
                       ROUND(AVG(CASE WHEN r.votes > 0 THEN r.helpful::decimal/r.votes ELSE 0 END), 2) as avg_usefulness
                FROM review r
                JOIN product_category pc ON r.asin = pc.asin
                JOIN category c ON pc.category_id = c.category_id
                GROUP BY c.name
                ORDER BY avg_usefulness DESC
                LIMIT 5;
                """
            )

            execute_query(cur,
                "Top 10 clientes que mais fizeram comentários por grupo de produto",
                """
                WITH ranked_customers AS (
                    SELECT g.name as group_name, r.customer_id, COUNT(*) as num_reviews,
                           ROW_NUMBER() OVER (PARTITION BY g.name ORDER BY COUNT(*) DESC) AS rn
                    FROM review r
                    JOIN product p ON r.asin = p.asin
                    JOIN product_group g ON p.group_id = g.group_id
                    GROUP BY g.name, r.customer_id
                )
                SELECT group_name, customer_id, num_reviews
                FROM ranked_customers
                WHERE rn <= 10
                ORDER BY group_name, num_reviews DESC;
                """
            )

    except Exception as e:
        print(f"[ERRO] Falha ao executar consultas: {e}")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
