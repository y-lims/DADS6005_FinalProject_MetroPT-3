#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

import argparse
import os
from time import sleep
import pandas as pd
import requests
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from pycaret.regression import load_model, predict_model
from sklearn.metrics import accuracy_score
from river import metrics, tree, anomaly, compose
import random

# Define online models
model_hoeff = tree.HoeffdingTreeClassifier(grace_period=100)
model_ex = tree.ExtremelyFastDecisionTreeClassifier(grace_period=100)

# Define accuracy metrics for online models
accuracy_hoeff = metrics.Accuracy()
accuracy_ex = metrics.Accuracy()
accuracy_sgt = metrics.Accuracy()

# Define lists to store actual and predicted labels for offline models
actual_labels_rf = []
predicted_labels_rf = []

actual_labels_lightgbm = []
predicted_labels_lightgbm = []

actual_labels_gbc = []
predicted_labels_gbc = []

# Define the User class to hold the deserialized data
class User:
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
    return User(**obj)

# Function to send data to Power BI API
def send_to_power_bi(data):
    url = "https://api.powerbi.com/beta/db5def6b-8fd8-4a3e-91dc-8db2501a6822/datasets/bb42339c-0983-437d-bdcf-69d4add3563a/rows?referrer=desktop&experience=power-bi&key=%2BA7LdQ71yhWeGAz2ij31jolQdeRg2smtnn8TZEdeLdDt4fD2MSzLW2IwR8p%2BAb2QjVwYkJ%2F2w%2FaPunawRrGJRA%3D%3D"
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, json=data, headers=headers)
    if response.status_code == 200:
        print("Data successfully sent to Power BI.")
    else:
        print(f"Failed to send data to Power BI. Status code: {response.status_code}, Response: {response.text}")

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

    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str, dict_to_user)

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

            if user is None:
                continue

            # Print the received values
            print(f"Received data: {vars(user)}")

            # Create a dataframe from the message data
            data = {
                'timestamp': user.timestamp, 'TP2': user.TP2, 'TP3': user.TP3,
                'H1': user.H1, 'DV_pressure': user.DV_pressure, 'Reservoirs': user.Reservoirs,
                'Oil_temperature': user.Oil_temperature, 'Motor_current': user.Motor_current,
                'COMP': user.COMP, 'DV_eletric': user.DV_eletric, 'Towers': user.Towers,
                'MPG': user.MPG, 'LPS': user.LPS, 'Pressure_switch': user.Pressure_switch,
                'Oil_level': user.Oil_level, 'Caudal_impulses': user.Caudal_impulses,
                'Severity': user.Severity  # Include Severity in the dataframe
            }

            df = pd.DataFrame([data])

            # Rename the dataframe columns to match the model's expected features
            df.columns = [
                'timestamp', 'TP2', 'TP3', 'H1', 'DV_pressure', 'Reservoirs',
                'Oil_temperature', 'Motor_current', 'COMP', 'DV_eletric', 'Towers',
                'MPG', 'LPS', 'Pressure_switch', 'Oil_level', 'Caudal_impulses', 'Severity'
            ]

            # Create a dictionary for Power BI
            data1 = {
                'timestamp': user.timestamp, 
                'TP2': user.TP2, 
                'TP3': user.TP3,
                'H1': user.H1, 
                'DV_pressure': user.DV_pressure, 
                'Reservoirs': user.Reservoirs,
                'Oil_temperature': user.Oil_temperature, 
                'Motor_current': user.Motor_current,
                'COMP': user.COMP,
                'DV_eletric': user.DV_eletric,
                'Towers': user.Towers,
                'MPG': user.MPG,
                'LPS': user.LPS, 
                'Pressure_switch': user.Pressure_switch,
                'Oil_level': user.Oil_level, 
                'Caudal_impulses': user.Caudal_impulses,
                'Severity' : user.Severity,
                'Predicted_Severity': None
            }

            # Offline model 1 (Random Forest Classifier)
            # Load Model_rf and make predictions
            saved_rf = load_model('Model_rf')
            predictions_rf = predict_model(saved_rf, data=df)
            print("\nPredicted_Model_rf", predictions_rf.iloc[0]['prediction_label'], " VS Actual=", user.Severity)
           
            predicted_labels_rf.append(predictions_rf.iloc[0]['prediction_label'])
            actual_labels_rf.append(user.Severity)
            offline_accuracy_rf = accuracy_score(actual_labels_rf, predicted_labels_rf)
            print("Accuracy (Offline_Model_rf):", offline_accuracy_rf)

            # Offline model 2 (Light Gradient Boosting Machine)
            # Load Model_lightgbm and make predictions
            saved_lightgbm = load_model('Model_lightgbm')
            predictions_lightgbm = predict_model(saved_lightgbm, data=df)
            print("\nPredicted_Model_lightgbm", predictions_lightgbm.iloc[0]['prediction_label'], " VS Actual=", user.Severity)
           
            predicted_labels_lightgbm.append(predictions_lightgbm.iloc[0]['prediction_label'])
            actual_labels_lightgbm.append(user.Severity)
            offline_accuracy_lightgbm = accuracy_score(actual_labels_lightgbm, predicted_labels_lightgbm)
            print("Accuracy (Offline_Model_lightgbm):", offline_accuracy_lightgbm)
           
            # Offline model 3 (Gradient Boosting Classifier)
            # Load Model_gbc and make predictions
            saved_gbc = load_model('Model_gbc')
            predictions_gbc = predict_model(saved_gbc, data=df)
            print("\nPredicted_Model_gbc", predictions_gbc.iloc[0]['prediction_label'], " VS Actual=", user.Severity)
           
            predicted_labels_gbc.append(predictions_gbc.iloc[0]['prediction_label'])
            actual_labels_gbc.append(user.Severity)
            offline_accuracy_gbc = accuracy_score(actual_labels_gbc, predicted_labels_gbc)
            print("Accuracy (Offline_Model_gbc):", offline_accuracy_gbc)

            # Online model 1 (HoeffdingTreeClassifier)
            y_pred_hoeff = model_hoeff.predict_one(data)
            model_hoeff.learn_one(data, user.Severity)
            print("\nOnline Prediction (Hoeffding) = ", y_pred_hoeff)

            accuracy_hoeff.update(user.Severity, y_pred_hoeff)
            print("Accuracy (Online Model Hoeffding):", accuracy_hoeff.get())

            # Online model 2 (ExtremelyFastDecisionTreeClassifier)
            y_pred_ex = model_ex.predict_one(data)
            model_ex.learn_one(data, user.Severity)
            print("\nOnline Prediction (ExtremelyFastDecision) = ", y_pred_ex)

            accuracy_ex.update(user.Severity, y_pred_ex)
            print("Accuracy (Online Model ExtremelyFastDecision):", accuracy_ex.get())
           
            # Compare accuracies and print the best model
            accuracies = {
            'Offline Model (Random Forest Classifier)': offline_accuracy_rf,
            'Offline Model (Light Gradient Boosting Machine)': offline_accuracy_lightgbm,
            'Offline Model (Gradient boosting Classifier)': offline_accuracy_gbc,
            'Online Model (HoeffdingTreeClassifier)': accuracy_hoeff.get(),
            'Online Model (ExtremelyFastDecisionTreeClassifier)': accuracy_ex.get(),
            }


            best_model = max(accuracies, key=accuracies.get)
            print(f"Best Model: {best_model} with Accuracy: {accuracies[best_model]}")
            print('\n')

            # Assign the predicted severity from the best model to the dictionary
            if best_model == 'Offline Model (Random Forest Classifier)':
                data1['Predicted_Severity'] = predictions_rf.iloc[0]['prediction_label']
            elif best_model == 'Offline Model (Light Gradient Boosting Machine)':
                data1['Predicted_Severity'] = predictions_lightgbm.iloc[0]['prediction_label']
            elif best_model == 'Offline Model (Gradient boosting Classifier)':
                data1['Predicted_Severity'] = predictions_gbc.iloc[0]['prediction_label']
            elif best_model == 'Online Model (HoeffdingTreeClassifier)':
                data1['Predicted_Severity'] = y_pred_hoeff
            elif best_model == 'Online Model (ExtremelyFastDecisionTreeClassifier)':
                data1['Predicted_Severity'] = y_pred_ex

            # Send data to Power BI
            send_to_power_bi([data1])

            sleep(3)
        except KeyboardInterrupt:
            break
    consumer.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroDeserializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True, help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True, help="Schema Registry (http(s)://host[:port])")
    parser.add_argument('-t', dest="topic", default="example_serde_avro", help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_avro", help="Consumer group")
    parser.add_argument('-p', dest="specific", default="true", help="Avro specific record")

    main(parser.parse_args())

# Example
# python avro_consumer.py -b "localhost:9092" -s "http://localhost:8081" -t "rawData"
