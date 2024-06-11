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


# A simple example demonstrating use of AvroDeserializer.

import argparse
import os

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from time import sleep
import pandas as pd
from pycaret.regression import load_model, predict_model
from sklearn.metrics import roc_auc_score, accuracy_score

from river import evaluate
from river import metrics
from river import tree

# Model of Online(real-time data)
model = tree.HoeffdingTreeClassifier(
        grace_period=100,
    )

# Accuracy of Online model
accuracy = metrics.Accuracy()

# Accuracy of Offline model
# Initialize an empty list to store actual and predicted labels
actual_labels = []
predicted_labels = []

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
        self.LPS = LPS
        self.Pressure_switch = Pressure_switch
        self.Oil_level = Oil_level
        self.Caudal_impulses = Caudal_impulses
        self.Severity = Severity

def dict_to_user(obj, ctx):

    if obj is None:
        return None

    return User(timestamp=obj['timestamp'], TP2=obj['TP2'], TP3=obj['TP3'], 
                H1=obj['H1'], DV_pressure=obj['DV_pressure'], Reservoirs=obj['Reservoirs'], 
                Oil_temperature=obj['Oil_temperature'], Motor_current=obj['Motor_current'], 
                COMP=obj['COMP'], DV_eletric=obj['DV_eletric'], Towers=obj['Towers'], MPG=obj['MPG'],
                LPS=obj['LPS'], Pressure_switch=obj['Pressure_switch'], Oil_level=obj['Oil_level'],
                Caudal_impulses=obj['Caudal_impulses'], Severity=obj['Severity'])

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

    sr_conf = {'url': args.schema_registry}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_user)

    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'group.id': args.group,
                     'auto.offset.reset': "latest"}

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])


    while True:
        try:
            # Poll for messages with a 1 second timeout
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            # Deserialize the message
            user = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            # Create a dataframe from the message data
            data = {'timstamp':user.timestamp, 'TP2':user.TP2, 'TP3':user.TP3,
                    'H1':user.H1, 'DV_pressure':user.DV_pressure, 'Reservoirs':user.Reservoirs,
                    'Oil_temperature':user.Oil_temperature, 'Motor_current' : user.Motor_current,
                    'COMP': user.COMP,'DV_eletric':user.DV_eletric,'Towers' : user.Towers,
                    'MPG': user.MPG,'LPS':user.LPS, 'Pressure_switch' : user.Pressure_switch,
                    'Oil_level':user.Oil_level, 'Caudal_impulses':user.Caudal_impulses
                    }

            df = pd.DataFrame(data, index=[user.timestamp])

            # Load the model and make predictions
            saved_rf = load_model('Model_rf')
            predictions = predict_model(saved_rf, data=df)
            
            print("Predicted", predictions.iloc[0]['prediction_label'], " VS Actual=", user.Severity)

            # Add the predicted label to the list
            predicted_labels.append(predictions.iloc[0]['prediction_label'])

            # Add the actual label to the list
            actual_labels.append(user.Severity)

            offline_accuracy = accuracy_score(actual_labels, predicted_labels)
            print("Accuracy (Offline):", offline_accuracy)
        
            '''# Calculate AUC score offline
            auc_score_offline = roc_auc_score([user.Severity], predictions.iloc[0]['Score_offline'])
            print("AUC Score (Offline):", auc_score_offline)'''
          
            # Online model
            y_pred = model.predict_one(data)
            model.learn_one(data, user.Severity)
            print("Online Prediction = ", y_pred)

            # Update accuracy metric
            accuracy.update(user.Severity, y_pred)

            # Print current accuracy
            print("Accuracy (Online):", accuracy.get())
                       
        except KeyboardInterrupt:
            break

        sleep(3)
    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroDeserializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_avro",
                        help="Consumer group")
    parser.add_argument('-p', dest="specific", default="true",
                        help="Avro specific record")

    main(parser.parse_args())

# Example
# python avro_consumer.py -b "localhost:9092" -s "http://localhost:8081" -t "rawData"