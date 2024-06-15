# [6005]_FinalProject_MetroPT-3
```
dataset: https://archive.ics.uci.edu/dataset/791/metropt+3+dataset
pycaret: https://pycaret.gitbook.io/docs/get-started/quickstart
```
1. python avro_producer.py -b "localhost:9092" -t "rawData" -s "http://localhost:8081"
2. curl -d @"sinkMysql_MetroPT.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors
    2.1. Open terminal in Docker
    2.2. mysql -uroot -p
    2.3. password: confluent
    2.4. show databases;
    2.5. use conenct_test;
    2.6. show tables;
    2.7. SELECT * FROM MetroPT;
3. python avro_consumer.py -b "localhost:9092" -s "http://localhost:8081" -t "rawData"
4. Open Power BI
