from sql_queries import drop_staging_tables_queries, create_staging_tables_queries
import psycopg2 

def create_database():
    """
    Creates the database on postgre and returns a connection and cursor to database   
    """
    # connect to postgres
    conn = psycopg2.connect(host='127.0.0.1', dbname='postgres', user='dataengineer', password='udacity')
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create a database with UTF8 encoding
    cur.execute('DROP DATABASE IF EXISTS cno')
    cur.execute("CREATE DATABASE cno WITH ENCODING 'utf-8' TEMPLATE template0")

    #changing database encoding to latin1
    #cur.execute("UPDATE pg_database SET ENCODING = pg_char_to_encoding('LATIN1') WHERE datname = 'cno'")
    
    # close connection
    conn.close()

    # connect to the cno database
    conn = psycopg2.connect(host='127.0.0.1', dbname='cno', user='dataengineer', password='udacity')
    cur = conn.cursor()

    return conn, cur

def drop_tables(conn, cur):
    """Drop tables if exists using the queries in 'drop_tables_queries' 

    Args:
        conn (obj): connection object to database
        cur (obj): cursor to interact with database
    """
    for query in drop_staging_tables_queries:
        cur.execute(query)
        conn.commit()

def create_tables(conn, cur):
    """Creates tables cnaes, vinculos and cno"

    Args:
        conn (obj): connection object to database
        cur (obj): cursor to interact with database
    """
    for query in create_staging_tables_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    - Drops (if exists) and creates CNO database
    - Establishes connection with the CNO database and gets a cursor
    - Drops all the tables
    - Creates all tables
    - Finally, closes the connection
    """
    conn, cur = create_database()
    print(conn)
    drop_tables(conn, cur)
    create_tables(conn, cur)

    conn.close()

if __name__ == '__main__':
    main()