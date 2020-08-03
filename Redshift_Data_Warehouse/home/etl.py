import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Function to the load the staging tables on redshift from JSON files in the song and log data folder.
       
       Arguments:
       cur: Cursor to the dwh db on redshift
       conn: connection to the dwh db on redshift
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Function to the load the final target analytical tables on redshift from the intermediate staging tables
       
       Arguments:
       cur: Cursor to the dwh db on redshift
       conn: connection to the dwh db on redshift
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()