#
# Copyright © 2023 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
"""Command line interface."""

import logging
import urllib

import click
import prometheus_client

from . import kafka

log = logging.getLogger(__name__)


def host_port(host_port_str):
    # Parse netloc like it is done for HTTP URLs.
    # This ensures that we will get the correct behavior for hostname:port
    # splitting even for IPv6 addresses.
    return urllib.parse.urlparse(f"http://{host_port_str}")


@click.command()
@click.option(
    "--prometheus",
    type=host_port,
    default=":8000",
    show_default=True,
    help="Hostname and port to listen on for Prometheus metric reporting",
)
@click.option(
    "--loglevel",
    type=click.Choice(logging._levelToName.values()),
    default="DEBUG",
    show_default=True,
    help="Log level",
)
@click.option("--bucketarn", help="Bucket ARN")
def main(prometheus, loglevel, bucketarn):
    """Monitor connectivity of a Kafka client.

    Specify the Kafka client configuration in environment variables using the
    conventions described in
    https://docs.confluent.io/platform/current/installation/docker/config-reference.html#confluent-enterprise-ak-configuration:

    * Start the environment variable name with `KAFKA_`.
    * Convert to upper-case.
    * Replace periods (`.`) with single underscores (`_`).
    * Replace dashes (`-`) with double underscores (`__`).
    * Replace underscores (`_`) with triple underscores (`___`).
    """
    logging.basicConfig(level=loglevel)

    prometheus_client.start_http_server(
        prometheus.port, prometheus.hostname or "0.0.0.0"
    )
    log.info("Prometheus listening on %s", prometheus.netloc)

    kafka.run(bucketarn)
