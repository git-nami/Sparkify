import configparser
import psycopg2
from sql_queries import select_table_queries

def select_tables(cur, conn):
    for query in select_table_queries:
        print(query)
        cur.execute(query)
        row = cur.fetchone()
        while row:
            print(row)
            row = cur.fetchone()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    select_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()