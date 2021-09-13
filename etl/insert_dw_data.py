from sql_queries import data_ingestion_queries
import psycopg2
from database_credentials import credentials

def data_ingest(conn, cur):
    """Insert data in the dimensions table and fact table.

    Args:
        conn (obj): connection object to database
        cur (obj): cursor to interact with database
    """
    for query in data_ingestion_queries:
        cur.execute(query)
        conn.commit()

def main():
    """Execute data_ingest method
    """
    
    db_username, db_password = credentials()
    conn = psycopg2.connect(host='127.0.0.1', dbname='cno', user=db_username, password=db_password)
    cur = conn.cursor()

    data_ingest(conn,cur)

if __name__ == '__main__':
    main()