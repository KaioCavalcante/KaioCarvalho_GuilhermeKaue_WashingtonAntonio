import sys
import psycopg

def log(msg):
    print(f"\n[DASHBOARD] {msg}")
    print("=" * 80)

def run(args):
    try:
        conn = psycopg.connect(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_pass
        )
    except Exception as e:
        print(f"[ERRO] Falha na conexão: {e}", file=sys.stderr)
        sys.exit(1)

    cur = conn.cursor()

    def execute_query(title, sql, params=None):
        log(title)
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        if not rows:
            print("Nenhum resultado encontrado.\n")
            return
        colnames = [desc.name for desc in cur.description]
        print("\t".join(colnames))
        for row in rows:
            print("\t".join([str(r) for r in row]))
        print("-" * 80)

    
    execute_query(
        "Top 5 comentários mais úteis (positivos) por produto",
        """
        SELECT r.asin, r.customer_id, r.rating, r.helpful, r.votes, r.review_date
        FROM review r
        WHERE r.votes > 0
        ORDER BY r.rating DESC, r.helpful DESC
        LIMIT 5
        """
    )

    
    execute_query(
        "Top 5 comentários mais úteis (negativos) por produto",
        """
        SELECT r.asin, r.customer_id, r.rating, r.helpful, r.votes, r.review_date
        FROM review r
        WHERE r.votes > 0
        ORDER BY r.rating ASC, r.helpful DESC
        LIMIT 5
        """
    )

    
    execute_query(
        "Produtos similares com melhor salesrank",
        """
        SELECT ps.asin, ps.similar_asin, p.title, p.salesrank
        FROM product_similar ps
        JOIN product p ON ps.similar_asin = p.asin
        JOIN product base ON base.asin = ps.asin
        WHERE p.salesrank IS NOT NULL AND base.salesrank IS NOT NULL
          AND p.salesrank < base.salesrank
        ORDER BY ps.asin, p.salesrank ASC
        """
    )

    
    execute_query(
        "Evolução diária da média de avaliações",
        """
        SELECT r.asin, r.review_date, AVG(r.rating) AS avg_rating
        FROM review r
        GROUP BY r.asin, r.review_date
        ORDER BY r.asin, r.review_date
        """
    )

    
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
        ORDER BY group_name, rn
        """
    )

    
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

    
    execute_query(
        "Top 5 categorias com maior média de avaliações úteis positivas",
        """
        SELECT c.name AS category,
               AVG(CASE WHEN r.votes > 0 THEN r.helpful::float / r.votes ELSE 0 END) AS avg_usefulness
        FROM category c
        JOIN product_category pc ON c.category_id = pc.category_id
        JOIN review r ON pc.asin = r.asin
        GROUP BY c.name
        ORDER BY avg_usefulness DESC
        LIMIT 5
        """
    )

    
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
    cur.close()
    conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Dashboard de consultas SQL para todos os produtos")
    parser.add_argument('--db-host', required=True)
    parser.add_argument('--db-port', type=int, required=True)
    parser.add_argument('--db-name', required=True)
    parser.add_argument('--db-user', required=True)
    parser.add_argument('--db-pass', required=True)
    args = parser.parse_args()

    try:
        run(args)
        sys.exit(0)
    except Exception as e:
        print(f"[ERRO] {e}", file=sys.stderr)
        sys.exit(1)
