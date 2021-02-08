import psycopg2
import psycopg2.extensions
import pandas as pd


class MyDataBase():
    """Class with principle functionalities to work with a database.

    Attributes:
    -----------
        conn: Connection.
        cur: Cursor.
    """

    def __init__(self, db="postgres", user="marina"):
        """Constructs all the necessary attributes for the database.

        :param db: (str) Database name.
        :param user: (str) User name.

        Isolation levels for psycopg2:
            0 = READ UNCOMMITTED
            1 = READ COMMITTED
            2 = REPEATABLE READ
            3 = SERIALIZABLE
            4 = DEFAULT

        Set the isolation level for the connection's cursors,
        will raise ActiveSqlTransaction exception otherwise
        """
        self.conn = psycopg2.connect(database=db, user=user)
        self.cur = self.conn.cursor()

        autocommit = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
        print("ISOLATION_LEVEL_AUTOCOMMIT:", psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        self.conn.set_isolation_level(autocommit)
        
    def close(self):
        """Close connection to database."""
        self.cur.close()
        self.conn.close()

    def create_data_base(self, db):
        """Create database with 'db' as name.
        Postgres does not support the condition IF NOT EXISTS in the CREATE DATABASE clause,
        however, IF EXISTS is supported on DROP DATABASE.
        Drop & recreate database.

        :param db: (str) Database name.
        """

        self.cur.execute('DROP DATABASE IF EXISTS db')
        self.cur.execute('CREATE DATABASE db')

        # Another option is to check the catalog first & branch the logic in python:
        self.cur.execute("""SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{}'""".format(db))
        exists = self.cur.fetchone()
        # if not exists:
        #     cursor.execute('CREATE DATABASE db')
        if exists:
            print('Database name = {}.'.format(db))

    def import_squema(self, file_name):
        """Import schema into database.

        :param file_name: (str) Input file with schema.
        """

        self.cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
        tables = self.cur.fetchall()

        if not tables:
            self.cur.execute(open(file_name, "r").read())

    def print_tables_names(self):
        """Print name of tables in database."""

        self.cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
        for table in self.cur.fetchall():
            print(table)

    def query(self, query):
        """Query something."""

        self.cur.execute(query)

    def fetchall(self):
        return self.cur.fetchall()

    def to_data_frame(self, query):
        table = pd.read_sql(query, self.conn)
        return table
