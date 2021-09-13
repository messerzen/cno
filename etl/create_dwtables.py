from sql_queries import drop_dw_tables_queries, create_dw_tables_queries
import psycopg2
from database_credentials import credentials

def drop_dw_tables(conn, cur):
    """Drop (if exists) the fact and the dimensions table of the datawarehouse.

    Args:
        conn (obj): connection object to database
        cur (obj): cursor to interact with database
    """

    for query in drop_dw_tables_queries:
        cur.execute(query)
        conn.commit()


def create_dw_tables(conn, cur):
    '''
    - Creates cnaes dimension table
    - Creates municipios dimension table
    - Creates obras fact table

    Args:
        conn (obj): connection object to database
        cur (obj): cursor to interact with database
    '''

    for query in create_dw_tables_queries:
        cur.execute(query)
        conn.commit()

def main():
    '''
    - Creates the data warehouse database and returns a cursor
    - Execute drop_dw_tables 
    - Execute create_dw_tables
    '''

    db_username, db_password = credentials()
    conn = psycopg2.connect(host='127.0.0.1', dbname='cno', user=db_username, password=db_password)
    cur = conn.cursor()

    drop_dw_tables(conn, cur)
    create_dw_tables(conn, cur)
    conn.close()

if __name__ == '__main__':
    main()    