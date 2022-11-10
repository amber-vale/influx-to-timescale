from influxdb import InfluxDBClient
import datetime


class InfluxDB():
    """InfluxDB client class that helps manage the connection to Influx
    """

    def __init__(self, host: str, port: int, username: str, password: str, database: str, ssl: bool = False, timeout: int = 60) -> None:
        self.client = InfluxDBClient(host=host, port=port, username=username,
                                     password=password, database=database, ssl=ssl, timeout=timeout)
        self.host = host
        self.database = database

    def get_client(self) -> InfluxDBClient:
        """Gets the InfluxDBClient that was configured

        Returns:
            InfluxDBClient: The Influx DB Client
        """

        return self.client

    def get_measurements(self):
        """Get all measurements on the selected database
        """

        # Get all measurements and then reformat into a list of strings
        raw_measurements = self.client.get_list_measurements()
        measurements = []
        for measurement in raw_measurements:
            measurements.append(measurement['name'])

        return measurements

    def get_shards(self):
        """Get all shards for the selected database.
        """

        shards = []
        raw_shards = self.client.query(f"show shards", database=self.database)

        # Process and transform results
        for shard in raw_shards[self.database]:

            shards.append(shard)

        return shards

    def get_time_range(self):
        """Gets the time range of data shards within the selected database.
        """

        shards = self.get_shards()
        oldest = None
        newest = None

        # Go through each shard and figure out the oldest and newest timestamps across the entire database
        for shard in shards:
            start_time = datetime.datetime.strptime(
                shard["start_time"], "%Y-%m-%dT%H:%M:%SZ")
            end_time = datetime.datetime.strptime(
                shard["end_time"], "%Y-%m-%dT%H:%M:%SZ")

            # Replace None with the current start/end of this shard
            if oldest is None:
                oldest = start_time
            if newest is None:
                newest = end_time

            # Compare against what we got already for oldest/newest with this shard's start/end times
            if oldest > start_time:
                oldest = start_time
            if newest < end_time:
                newest = end_time

        return oldest, newest

    def get_fields(self, measurement: str):
        """Gets all fields from a measurement along with datatypes

        Args:
            measurement (str): The measurement name
        """

        # Drop everything after the first space to prevent injection attacks
        measurement = measurement.split(" ")[0]

        # Perform query
        field_keys = self.client.query(
            f"show field keys from {measurement}", database=self.database)

        # Transform ResultSet to dict
        fields = {}
        for field in field_keys[measurement]:
            fields[field["fieldKey"]] = field["fieldType"]

        return fields

    def get_tags(self, measurement: str):
        """Gets all tags from a measurement along with datatypes

        Args:
            measurement (str): The measurement name
        """

        # Drop everything after the first space to prevent injection attacks
        measurement = measurement.split(" ")[0]

        # Perform query
        tag_keys = self.client.query(
            f"show tag keys from {measurement}", database=self.database)

        # Transform ResultSet to dict
        tags = {}
        for tag in tag_keys[measurement]:
            tags[tag["tagKey"]] = "string"

        return tags

    def get_hypertable_columns(self, measurement: str, float_precision: int = 2):
        """Generate the hypertable columns that match the schema of the specified measurement

        Args:
            measurement (str): The measurement name
            float_precision (int, optional): The precision to use with FLOAT(n) columns. Defaults to 2.
        """

        INFLUX_TYPES_TO_COLUMN_TYPES = {
            "string": "TEXT",
            "float": f"FLOAT({float_precision})",
            "integer": "INT",
            "boolean": "BOOL"
        }
        columns = ["time TIMESTAMPTZ NOT NULL"]

        # Get the fields and tags from the measurement
        get_fields = self.get_fields(measurement)
        get_tags = self.get_tags(measurement)

        # Convert fields to columns
        for field_name in get_fields:
            columns.append(
                f"{field_name} {INFLUX_TYPES_TO_COLUMN_TYPES.get(get_fields[field_name], 'TEXT')}")
        # Convert tags to columns
        for tag_name in get_tags:
            columns.append(
                f"{tag_name} {INFLUX_TYPES_TO_COLUMN_TYPES.get(get_tags[tag_name], 'TEXT')}")

        return columns
