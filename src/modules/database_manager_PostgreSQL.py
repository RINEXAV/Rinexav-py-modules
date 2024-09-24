import psycopg2


class DatabaseManager:
    """
    A class with functions that allow you to manage PostgreSQL databases from a Python script.

    :param dbname: Database name.
    :param user: Name of the user responsible for the database.
    :param password: Database password.
    :param host: IP number or hostname of the database.
    :param port: The port number on which the database is located.
    """
    def __init__(self, dbname, user, password, host, port):
        self.connection = psycopg2.connect(dbname = dbname, user = user, password = password, host = host, port = port)
        self.cursor = self.connection.cursor()

    def create_table(self, sql: str):
        """
        Creating a new table in the database. Creating a new table in the database.
        Any SQL query can be entered in the function, so it is a universal function.

        :param sql: SQL query compatible with PostgreSQL syntax.

        :return: Executing an SQL query on the database server side.
        """
        self.cursor.execute(sql)
        self.connection.commit()

    def insert(self, table, *values):
        """
        Adding new values ​​to a table in the database.

        :param table: Table name.
        :param *values: Values ​​according to the order of the columns in the table.

        :return: Executing an SQL query on the database server side.
        """
        values_placeholder = ','.join(['%s' for _ in values])
        self.cursor.execute(f"INSERT INTO {table} VALUES ({values_placeholder})", values)
        self.connection.commit()
        
    def fetch_all(self, table, **conditions):
        """
        Returns values ​​contained in a database table.

        :param table: Table name.
        :param **conditions: Conditions based on which values ​​will be selected from the table.

        :return: Executing an SQL query on the database server side.
        """
        conditions_placeholder = ' AND '.join([f'{condition}=%s' for condition in conditions])
        self.cursor.execute(f"SELECT * FROM {table} WHERE {conditions_placeholder}", tuple(conditions.values()))
        return self.cursor.fetchall()
        
    def __del__(self):
        self.connection.close()