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
import requests 

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
actual_labels_rf = []
predicted_labels_rf = []

actual_labels_lightgbm = []
predicted_labels_lightgbm = []

actual_labels_et = []
predicted_labels_et = []

api_url = "https://api.powerbi.com/beta/db5def6b-8fd8-4a3e-91dc-8db2501a6822/datasets/e26df6ad-6564-4460-9de4-f7163a75b6fc/rows?referrer=desktop&experience=power-bi&key=IYrPBZ3F0rY4sHFFOVZoCO1gzB0KtIeimqBvBqg2eHxV%2BXt%2BD7NTmOuEtXqIXD75f27%2BGIzbSppGQUlyGAR0rg%3D%3D"

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
            '''print(df)
            
            # Offline model 1
            # Load Model_rf and make predictions
            saved_rf = load_model('Model_rf')
            predictions_rf = predict_model(saved_rf, data=df)
            print("\nPredicted_Model_rf", predictions_rf.iloc[0]['prediction_label'], " VS Actual=", user.Severity)
            
            predicted_labels_rf.append(predictions_rf.iloc[0]['prediction_label'])
            actual_labels_rf.append(user.Severity)
            offline_accuracy_rf = accuracy_score(actual_labels_rf, predicted_labels_rf)
            print("Accuracy (Offline_Model_rf):", offline_accuracy_rf)

            # Offline model 2
            # Load Model_lightgbm and make predictions
            saved_lightgbm = load_model('Model_lightgbm')
            predictions_lightgbm = predict_model(saved_lightgbm, data=df)
            print("\nPredicted_Model_lightgbm", predictions_lightgbm.iloc[0]['prediction_label'], " VS Actual=", user.Severity)
            
            predicted_labels_lightgbm.append(predictions_lightgbm.iloc[0]['prediction_label'])
            actual_labels_lightgbm.append(user.Severity)
            offline_accuracy_lightgbm = accuracy_score(actual_labels_lightgbm, predicted_labels_lightgbm)
            print("Accuracy (Offline_Model_lightgbm):", offline_accuracy_lightgbm)
            
            # Offline model 3
            # Load Model_et and make predictions
            saved_et = load_model('Model_et')
            predictions_et = predict_model(saved_et, data=df)
            print("\nPredicted_Model_et", predictions_et.iloc[0]['prediction_label'], " VS Actual=", user.Severity)
            
            predicted_labels_et.append(predictions_et.iloc[0]['prediction_label'])
            actual_labels_et.append(user.Severity)
            offline_accuracy_et = accuracy_score(actual_labels_et, predicted_labels_et)
            print("Accuracy (Offline_Model_et):", offline_accuracy_et)

            # Online model 1
            y_pred = model.predict_one(data)
            model.learn_one(data, user.Severity)
            print("\nOnline Prediction (HoeffdingTreeClassifier) = ", y_pred)

            accuracy.update(user.Severity, y_pred)

            print("Accuracy (Online):", accuracy.get())

            # Online model 2
            models = [('LogisticRegression', linear_model.LogisticRegression),
            ('PAClassifier', linear_model.PAClassifier)]'''

            # Data to insert
            data_to_insert = {
                "TP2": data['TP2']
            }
            # Make POST request to insert data
            response = requests.post(api_url, json=data_to_insert)

            if response.status_code == 200:
                print("Data inserted successfully.")
            else:
                print("Failed to insert data. Status code:", response.status_code)
                print("Response content:", response.text)

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