#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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


# A simple example demonstrating use of AvroSerializer.
from confluent_kafka.admin import AdminClient, NewTopic
import argparse
import os
import requests
import pandas as pd 

from time import sleep

from uuid import uuid4

from six.moves import input

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


class User(object):

    def __init__(self, timestamp, TP2, TP3, H1, DV_pressure, Reservoirs, Oil_temperature, Motor_current, 
                 COMP, DV_eletric, Towers, MPG, LPS, Pressure_switch, Oil_level, Caudal_impulses, Severity):
        self.timestamp = timestamp
        self.TP2 = TP2
        self.TP3 = TP3
        self.H1 = H1
        self.DV_pressure = DV_pressure
        self.Reservoirs = Reservoirs
        self.Oil_temperature = Oil_temperature
        self.Motor_current = Motor_current
        self.COMP = COMP
        self.DV_eletric = DV_eletric
        self.Towers = Towers
        self.MPG = MPG
        self.LPS =LPS
        self.Pressure_switch = Pressure_switch
        self.Oil_level = Oil_level
        self.Caudal_impulses = Caudal_impulses
        self.Severity = Severity


def user_to_dict(user, ctx):
    
    # User._address must not be serialized; omit from dict
    return dict(timestamp=user.timestamp, TP2=user.TP2, TP3=user.TP3, H1=user.H1, DV_pressure=user.DV_pressure,
                Reservoirs=user.Reservoirs, Oil_temperature=user.Oil_temperature, Motor_current=user.Motor_current, COMP=user.COMP,
                DV_eletric=user.DV_eletric, Towers=user.Towers, MPG=user.MPG, LPS=user.LPS, Pressure_switch=user.Pressure_switch,
                Oil_level=user.Oil_level, Caudal_impulses=user.Caudal_impulses, Severity=user.Severity) 


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(args):
    topic = args.topic
    is_specific = args.specific == "true"

    if is_specific:
        schema = "user_specific.avsc"
    else:
        schema = "user_generic.avsc"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema}") as f:
        schema_str = f.read()

    schema_registry_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     user_to_dict)

    string_serializer = StringSerializer('utf_8')

    producer_conf = {'bootstrap.servers': args.bootstrap_servers}

    producer = Producer(producer_conf)

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    
    
    api = r"D:\NIDA\DADS6005\Finalproject\avro_kafka_example1\Online_data.csv"

    
    #Create topic with a number of partition and replicas
    admin_client = AdminClient(producer_conf)
    topic_list = []
    topic_list.append(NewTopic(topic, 2, 2))
    admin_client.create_topics(topic_list)

  
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        
        df = pd.read_csv(r"D:\NIDA\DADS6005\Finalproject\avro_kafka_example1\Online_data.csv")
        for i in range(len(df)):
            data = df.iloc[i, :]
            print(data)

            user = User(timestamp=str(data["timestamp"]), TP2=float(data["TP2"]), TP3= float(data["TP3"]),
                        H1=float(data["H1"]),DV_pressure=float(data["DV_pressure"]), 
                        Reservoirs=float(data["Reservoirs"]), Oil_temperature=float(data["Oil_temperature"]), 
                        Motor_current=float(data["Motor_current"]), COMP=float(data["COMP"]), DV_eletric=float(data["DV_eletric"]),
                        Towers=float(data["Towers"]), MPG=float(data["MPG"]), LPS=float(data["LPS"]),
                        Pressure_switch=float(data["Pressure_switch"]), Oil_level=float(data["Oil_level"]), 
                        Caudal_impulses=float(data["Caudal_impulses"]), Severity=str(data["Severity"]))
                
            producer.produce(topic=topic,
                                key="timestamp",
                                value=avro_serializer(user, SerializationContext(topic, MessageField.VALUE)),
                                on_delivery=delivery_report)
        
            sleep(3)
            producer.poll(0.0)
        
        print("\nFlushing records...")
        producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroSerializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-p', dest="specific", default="true",
                        help="Avro specific record")

    main(parser.parse_args())
    # python avro_producer.py -b "localhost:9092" -t "rawData" -s "http://localhost:8081"