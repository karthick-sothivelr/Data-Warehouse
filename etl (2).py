import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load JSON input data (log_data, song_data) from S3
    Insert data into stagging tables (staging_events, staging_songs)
    
    Arguments:
    cur -- cursor to connected DB (Allows to execute SQL commands)
    conn -- connection to Postgres database    
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables into star schema tables
    
    Arguments:
    cur -- cursor to connected DB (Allows to execute SQL commands)
    conn -- connection to Postgres database    
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connect to database and run load_staging_tables to load data from JSON files into staging tables and 
    run insert_tables to insert data in star schema tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()