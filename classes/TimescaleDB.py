import psycopg2


class TimescaleDB:
    """A wrapper around psycopg2 that makes it easier to interact with TimescaleDB specific functionality along with normal PostgreSQL.
    """

    def __init__(self, postgres_connection_str: str) -> None:
        try:
            self.connection: psycopg2 = psycopg2.connect(
                postgres_connection_str)
        except psycopg2.OperationalError as e:
            print(f"Unable to connect to your TimescaleDB.\r\n{e}")

    def get_cursor(self):
        """Get a cursor from the current connection
        """
        return self.connection.cursor()

    def create_hypertable(self, name: str, columns: list, time_column_name: str = "time", dry_run: bool = False):
        """Creates a new hypertable with the specified columns list. 
        !!! This is not SQL injection safe - do not pass unsanitized data into this function. !!!

        Args:
            name (str): The name of the table.
            columns (list): The list of column definitions. (Ex: <field> <datatype> <extra options>: time TIMESTAMPTZ NOT NULL)
            time_column_name (str, optional): The field of the time field to use for the hypertable create command. Defaults to "time".
            dry_run (bool, optional): If True, this command does not actually create the table but instead returns all the commands that it would execute. Defaults to False.
        """

        dry_run_commands = []
        cursor = self.get_cursor()

        # Create the base table first
        create_table_query = f"CREATE TABLE {name} ("
        for index, column in enumerate(columns):
            create_table_query += f"{column}"  # Add the column definition
            # Add the trailing column if we are not at the end of the list
            create_table_query += ", " if index < len(columns) - 1 else ""
        create_table_query += ");"

        if not dry_run:
            try:
                cursor.execute(create_table_query)
            except psycopg2.Error as e:
                # Handle already existing tables and skip this step
                if "already exists" in str(e):
                    cursor.execute("ROLLBACK")
                    print(
                        f"Basic table '{name}' already exists, skipping basic creation...")
                else:  # For other errors, fall back to the original error and abort the entire command
                    print(f"Unable to create basic table '{name}':\r\n{e}")
                    return False
        else:
            dry_run_commands.append(create_table_query)

        # Now create the hypertable
        create_hypertable_query = f"SELECT create_hypertable('{name}', '{time_column_name}')"
        if not dry_run:
            try:
                cursor.execute(create_hypertable_query)
            except psycopg2.Error as e:
                # Handle if the hypertable already exists and skip this step
                if "already a hypertable" in str(e):
                    cursor.execute("ROLLBACK")
                    print(
                        f"Hypertable '{name}' already exists, skipping hypertable creation...")
                else:  # Show the error message and abort the command if something else
                    print(
                        f"Unable to create hypertable '{name}' using time column '{time_column_name}':\r\n{e}")
                    return False
        else:
            dry_run_commands.append(create_hypertable_query)

        # Commit and cleanup
        self.connection.commit()
        cursor.close()

        if not dry_run:
            return True
        else:
            return dry_run_commands
