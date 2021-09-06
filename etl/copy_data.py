import psycopg2
from sql_queries import copy_data_queries

def copy_data(conn, cur):
    """Copy data from csv files to database tables

    Args:
        conn (obj): connection object to database
        cur (obj): cursor to interact with database
    """
    for query in copy_data_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    - Creates a connection with cno database and returns a cursor
    - Executes copy_data method.
    """

    conn = psycopg2.connect(host='127.0.0.1', dbname='cno', user='dataengineer', password='udacity')
    cur = conn.cursor()
    copy_data(conn, cur)
    
    # Closes connection
    conn.close()

if __name__ == '__main__':
    main()