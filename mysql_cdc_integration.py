import mysql.connector
import json
import time


class MySQLCDCConnector:
    def __init__(self, host, port, database, username, password):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.connection = None
        self.cursor = None
        self.last_processed_time = 0

    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.username,
            password=self.password,
            auth_plugin='mysql_native_password'
        )
        self.cursor = self.connection.cursor()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.disconnect()

    def read_changes(self):
        query = "SELECT * FROM cdc_table WHERE timestamp > %s"
        self.cursor.execute(query, (self.last_processed_time,))
        rows = self.cursor.fetchall()

        changes = []
        for row in rows:
            change = {
                'timestamp': row[0],
                'operation': row[1],
                'data': json.loads(row[2])
            }
            changes.append(change)

        if rows:
            self.last_processed_time = max(rows, key=lambda x: x[0])[0]

        return changes

    def process_changes(self):
        while True:
            changes = self.read_changes()
            if changes:
                for change in changes:
                    # Process the change and perform necessary operations
                    # Example: Send the change to Dozer for processing
                    print(f"Processing change: {change}")
            else:
                # Sleep for 1 second before checking for new changes again
                time.sleep(1)

# Comprehensive Tests


def test_mysql_cdc_connector():
    # Create a test MySQL database and table for CDC
    # Insert some sample data into the CDC table

    connector = MySQLCDCConnector(
        host='localhost',
        port=3306,
        database='my_cdc_db',
        username='root',
        password='password'
    )

    # Test connection and disconnection
    connector.connect()
    assert connector.connection.is_connected()
    connector.disconnect()
    assert not connector.connection.is_connected()

    # Test reading changes
    connector.connect()
    changes = connector.read_changes()
    assert isinstance(changes, list)

    # Test processing changes
    connector.process_changes()

    connector.disconnect()


if __name__ == '__main__':
    test_mysql_cdc_connector()
