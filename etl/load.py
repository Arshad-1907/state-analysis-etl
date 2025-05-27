import pandas as pd
import sqlite3

def load_to_sqlite():
    df = pd.read_csv('data/output/output.csv')
    conn = sqlite3.connect('data/output/state_analysis.db')
    df.to_sql('state_analysis', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Loaded {len(df)} rows into 'state_analysis' table in SQLite database.")

if __name__ == "__main__":
    load_to_sqlite()
