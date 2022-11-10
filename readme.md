# influx-to-timescale

A Python script to migrate your [Influx](https://www.influxdata.com/) data over to [Timescale](https://www.timescale.com/).

Currently designed and tested against Influx 1.x.

## Why not [outflux](https://github.com/timescale/outflux)?

* [Hasn't been touched since 2020](https://github.com/timescale/outflux/commits/develop)
* [Queries against InfluxDB super inefficiently](https://github.com/timescale/outflux/issues/54) by not using time ranges which is [bad for Influx](https://docs.influxdata.com/platform/troubleshoot/oom-loops/#unoptimized-queries)
* [Does not](https://github.com/timescale/outflux/issues/92) [support Timescale 2](https://github.com/timescale/outflux/issues/85)

## Getting started

