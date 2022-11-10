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
    """Migrate InfluxDB data over to TimescaleDB
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
@click.option("--dump", help="Dump the shard data into console.", is_flag=True, default=False)
@click.pass_context
def shards(ctx, dump: bool):
    """Lists all shards found in Influx.
    """
    # Get clients and fields from the click context
    influx: InfluxDB = ctx.obj.get("influx_client")

    click.echo(f"Looking for shards...")
    # Get all fields and tags and then merge together
    shards = influx.get_shards()

    # Print results
    if dump:
        click.echo(shards)
    click.echo(
        f"\r\nFound {len(shards)} shards.")


@cli.command()
@click.pass_context
def timerange(ctx):
    """Shows the oldest and newest data in the Influx database.
    """
    # Get clients and fields from the click context
    influx: InfluxDB = ctx.obj.get("influx_client")

    click.echo(f"Looking for shards...\r\n")
    # Get all fields and tags and then merge together
    oldest, newest = influx.get_time_range()

    # Print results
    click.echo(f"Oldest: {oldest}")
    click.echo(f"Newest: {newest}")


if __name__ == '__main__':
    cli(obj={})
