#
# Copyright © 2023 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
"""Monitor Kafka consumer connectivity."""

import json
import logging
import re

import boto3
import gcn_kafka

from . import metrics

log = logging.getLogger(__name__)
s3_client = boto3.client("s3")


def stats_cb(data):
    stats = json.loads(data)
    for broker in stats["brokers"].values():
        metrics.broker_state.labels(broker["name"]).state(broker["state"])


def sanitize_topic(topic):
    """Replaces invalid characters in the topic string so it can be used as a file name

    Args:
        topic (string): Kafka message topic

    Returns:
        safe_name: A formatted version of the topic string with underscores(`_`) in place of illegal characters
    """
    invalid_chars = r'[\/:*?"<>|]'
    safe_name = re.sub(invalid_chars, "_", topic)
    return safe_name.strip()


def parse_into_s3_object(message):
    """Parses a Kafka message into a key and body.

    Args:
        message (Message): Any Kafka message

    Returns:
        key (string): The Key for S3's put_object method, formatted as `topics/<topic>/<encodedPartition>/<topic>+<kafkaPartition>+<startOffset>.<format>` defined at https://docs.confluent.io/kafka-connectors/s3-sink/current/overview.html#s3-object-names

        body (dict): The Kafka message converted to a dict to be JSON serializable to include all fields available, such as:
        - error
        - headers
        - key
        - latency
        - leader_epoch
        - offset
        - partition
        - timestamp
        - topic
        - value

    """
    topic = sanitize_topic(message.topic())
    offset = message.offset()
    partition = message.partition()
    key = f"topics/{topic}/partition={partition}/{topic}+{partition}+{offset}.json"
    properties = [
        method_name
        for method_name in dir(message)
        if not method_name.startswith("__") and not method_name.startswith("set_")
    ]
    return key, dict(
        [
            (
                messageKey,
                (
                    getattr(message, messageKey)().decode()
                    if isinstance(attr_value := getattr(message, messageKey)(), bytes)
                    and callable(getattr(message, messageKey))
                    else attr_value()
                    if callable(attr_value)
                    else attr_value
                ),
            )
            for messageKey in properties
        ]
    )


def run(bucketarn):
    log.info("Creating consumer")
    config = gcn_kafka.config_from_env()
    config["stats_cb"] = stats_cb
    config["statistics.interval.ms"] = 1e3
    consumer = gcn_kafka.Consumer(config)

    log.info("Subscribing")
    topics = list(consumer.list_topics().topics.keys())
    consumer.subscribe(topics)

    log.info("Entering consume loop")
    while True:
        for message in consumer.consume(timeout=1):
            topic = message.topic()
            key, body = parse_into_s3_object(message)
            s3_client.put_object(
                Bucket=bucketarn.replace("arn:aws:s3:::", ""),
                Key=key,
                Body=body,
            )
            if error := message.error():
                log.error("topic %s: got error %s", topic, error)
            else:
                log.info("topic %s: got message", topic)
                partition = message.partition()
                metrics.received.labels(topic, partition).inc()
