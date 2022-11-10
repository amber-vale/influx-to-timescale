from classes.InfluxDB import InfluxDB
from classes.TimescaleDB import TimescaleDB
import os
import click
from dotenv import load_dotenv
load_dotenv()


@click.group()
@click.option("--influx-host", default=lambda: os.environ.get("INFLUX_HOST", "localhost"), type=str, help="The influx host address. Default is localhost or $INFLUX_HOST", required=True)
@click.option("--influx-port", default=lambda: os.environ.get("INFLUX_PORT", 8086), help="The Influx port. Default is 8086 or $INFLUX_PORT", type=int)
@click.option("--influx-user", default=lambda: os.environ.get("INFLUX_USER", None), help="The Influx username. Default is None or $INFLUX_USER", type=str)
@click.option("--influx-pass", default=lambda: os.environ.get("INFLUX_PASS", None), help="The Influx password. Default is None or $INFLUX_PASS", type=str)
@click.option("--postgres-conn", default=lambda: os.environ.get("POSTGRES_CONN", None), help="The Postgres connection string. Default is None or $POSTGRES_CONN", type=str)
@click.option("--database", default=lambda: os.environ.get("INFLUX_DB", None), help="The Influx database to use. Default is $INFLUX_DB", type=str, required=True)
@click.option("--measurement", default=lambda: os.environ.get("INFLUX_MEASUREMENT", None), help="The Influx measurement name to use. Default is $INFLUX_MEASUREMENT.", type=str, required=True)
@click.pass_context
def cli(ctx, influx_host: str, influx_port: int, influx_user: str, influx_pass: str, postgres_conn: str, database: str, measurement: str):
    """Analyze and migrate InfluxDB schema over to TimescaleDB
    """

    # Get all parameters passed from either the environment or command line
    ctx.ensure_object(dict)
    ctx.obj["influx_host"] = influx_host
    ctx.obj["influx_port"] = influx_port
    ctx.obj["influx_user"] = influx_user
    ctx.obj["influx_pass"] = influx_pass
    ctx.obj["postgres_conn"] = postgres_conn
    ctx.obj["database"] = database
    ctx.obj["measurement"] = measurement

    # Setup influx and timescale clients
    ctx.obj["influx_client"] = InfluxDB(
        influx_host, influx_port, influx_user, influx_pass, database)
    ctx.obj["timescale_client"] = TimescaleDB(postgres_conn)


@cli.command()
@click.pass_context
def measurements(ctx):
    """Lists all measurements found in Influx.
    """
    # Get clients and fields from the click context
    influx: InfluxDB = ctx.obj.get("influx_client")

    # Get all fields and tags and then merge together
    measurements = influx.get_measurements()

    # Print results
    click.echo(
        f"All Influx measurements found in {influx.database} on {influx.host}:")
    click.echo("\r\n".join(measurements))


@cli.command()
@click.pass_context
def analyze(ctx):
    """Analyzes the current InfluxDB measurement schema and reports back all fields and tags.
    """
    # Get clients and fields from the click context
    measurement = ctx.obj.get("measurement")
    influx: InfluxDB = ctx.obj.get("influx_client")

    # Get all fields and tags and then merge together
    fields = influx.get_fields(measurement)
    tags = influx.get_tags(measurement)
    all_fields = {**fields, **tags}

    # Print results
    click.echo("All detected fields and tags:")
    for field_name in all_fields:
        click.echo(f"{field_name} ({all_fields[field_name]})")


@cli.command()
@click.option("-d", "--dry-run", type=bool, is_flag=True, help="Do a dry run and output the migration plan.")
@click.option("--float-precision", type=int, default=2, help="The precision to use with FLOAT(n) columns. Default is 2.")
@click.pass_context
def migrate(ctx, dry_run: bool, float_precision: int):
    """Migrates the Influx measurement over to a Timescale hypertable.
    """

    if dry_run:
        click.echo(
            f"This migration will be dry run, no schema or data will be migrated at this time.\r\n\r\n")

    # Get needed fields and clients from click context
    measurement = ctx.obj.get("measurement")
    influx: InfluxDB = ctx.obj.get("influx_client")
    timescale: TimescaleDB = ctx.obj.get("timescale_client")

    # Convert measurement to columns
    hypertable_columns = influx.get_hypertable_columns(
        measurement, float_precision=float_precision)

    # Do dry run of create hypertable
    commands = timescale.create_hypertable(
        measurement, hypertable_columns, dry_run=dry_run)

    # Echo commands and text for dry run
    if dry_run:
        click.echo(
            f"Schema migration plan of '{measurement}' completed. Here's the commands that will be executed: \r\n")
        for index, command in enumerate(commands):
            click.echo(f"#{index}: {command}")

        click.echo(f"\r\nThe commands above may change if the Influx measurement schema changes between now and when you run the schema migrate command.\r\nNo actual data will be migrated. You will have to create any additional indexes on your columns to improve performance.")
        return

    if commands:
        click.echo(
            f"The schema of measurement '{measurement}' was migrated successfully! No data was migrated.")
    else:
        click.echo(
            f"\r\nThere was an error while migrating the measurement. Look above this message for error messages.")


if __name__ == '__main__':
    cli(obj={})
