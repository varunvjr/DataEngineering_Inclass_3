#!/usr/bin/env python
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
from uuid import uuid4
import time
import random

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer_conf['transactional.id'] = 'transaction-aware-producer'
    producer = Producer(producer_conf)

    # Initialize producer transaction.
    producer.init_transactions()
    # Start producer transaction.
    producer.begin_transaction()

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    jsonFile = open('bcsample.json')
    data = json.load(jsonFile)

    count = 0
    for n in range(len(data)):
        record_key = str(uuid4())
        record_value = json.dumps(data[n])
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        producer.poll(0)
        count = count + 1;
        if count % 4 == 0:
           time.sleep(2)

           if random.choice([True, False]):
              # If True, Commit the transaction
              print('Going to commit the transaction')
              producer.commit_transaction()

              # Begin new transaction
              producer.begin_transaction()
           else:
              # else abort transaction
              print('Going to abort the transaction')
              producer.abort_transaction()

              # Begin new transaction
              producer.begin_transaction()

    producer.flush()
    print("{} messages were produced to topic {}!".format(delivered_records, topic))
