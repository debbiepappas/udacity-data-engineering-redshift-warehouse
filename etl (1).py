import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# The load_staging_tables function will call queries to load the sparkify data from the json files
# which reside in the AWS S3 buckets into the event and song staging tables.  The staging_events_copy query will copy
# data using the 's3://udacity-dend/log_json_path.json' file. The staging_songs_copy query will copy data using the 
# s3 path 's3://udacity-dend/song_data'.  

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

# The insert_tables will call queries to insert data into the fact and dimension tables from the event 
# and song staging tables. 
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

# The main function will connect to the redshift cluster which has already been created and started using host, 
# dbname, user name, password and port number referenced in the 'dwh.cfg' file and then call the load_staging_tables
# and insert_tables functions. 
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()