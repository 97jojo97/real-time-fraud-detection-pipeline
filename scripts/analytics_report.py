import os
import pandas as pd
from sqlalchemy import create_engine, text

# DB connection string
DB_URI = os.environ.get("DATABASE_URL", "postgresql://admin:admin@localhost:5432/fraud_db")
engine = create_engine(DB_URI)

def run_query(title, sql):
    print(f"\n{'='*60}")
    print(f"[+] ANALYTICS REPORT: {title}")
    print(f"{'='*60}")
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
            if df.empty:
                print("    -> No results found.")
            else:
                print(df.to_string(index=False))
    except Exception as e:
        print(f"[-] Query failed: {e}")

def main():
    # ---------------------------------------------------------
    # QUERY 2: User Risk Profiles (Aggregations + Sorting)
    # ---------------------------------------------------------
    q2 = """
    SELECT 
        id as user_id, 
        current_age, 
        yearly_income, 
        credit_score,
        num_credit_cards
    FROM users
    WHERE credit_score < 600 AND yearly_income::float > 50000
    ORDER BY yearly_income DESC
    LIMIT 10;
    """

    # ---------------------------------------------------------
    # QUERY 3: Dark Web Compromise (Case Statements + Count)
    # ---------------------------------------------------------
    q3 = """
    SELECT 
        card_brand,
        COUNT(*) AS total_cards,
        SUM(CASE WHEN card_on_dark_web = 'Yes' THEN 1 ELSE 0 END) AS compromised_count,
        ROUND((SUM(CASE WHEN card_on_dark_web = 'Yes' THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) as compromise_pct
    FROM cards
    GROUP BY card_brand
    ORDER BY compromised_count DESC;
    """

    run_query("USER RISK PROFILES (Targeting Bad Credit Scores)", q2)
    run_query("CARD BRAND VULNERABILITY (Dark Web Statistics)", q3)

if __name__ == "__main__":
    main()
