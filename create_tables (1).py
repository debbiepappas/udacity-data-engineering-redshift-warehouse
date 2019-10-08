import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


# The drop_tables function will call the queries to drop the fact and dimension tables
# in the sparkify database if they exist. This is the first step in refreshing the database.
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# The create_tables function will call the queries to create the fact and dimension
# tables in the sparkify database.
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# The main function connects to the redshift cluster which has already been created and started using the host,
# dbname, user name, password and port number referenced in 'dwh.cfg' and then calls the drop_tables and
# create_tables functions.
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
                                                                                       
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()