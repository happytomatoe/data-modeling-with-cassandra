from cassandra.cluster import Cluster

from sql_queries import DROP_TABLE_QUERIES, CREATE_TABLE_QUERIES


def create_keyspace():
    """

    :return:
    """
    cluster = Cluster()

    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sparkify
          WITH REPLICATION = { 
           'class' : 'SimpleStrategy', 
           'replication_factor' : 1 
          };
        """)
    session.set_keyspace("sparkify")
    return cluster, session


def drop_tables(session):
    """
    Drops each table using the queries in `DROP_TABLE_QUERIES` list.
    """
    for query in DROP_TABLE_QUERIES:
        session.execute(query)


def create_tables(session):
    """
    Creates each table using the queries in `CREATE_TABLE_QUERIES` list.
    """
    for query in CREATE_TABLE_QUERIES:
        session.execute(query)


def main():
    """
    - Drops (if exists) and Creates the sparkify database.

    - Establishes connection with the sparkify database and gets
    session.

    - Drops all the tables.

    - Creates all tables needed.

    - Finally, shutdowns the session and connection to cluster.
    """
    cluster, session = create_keyspace()

    drop_tables(session)
    create_tables(session)

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
