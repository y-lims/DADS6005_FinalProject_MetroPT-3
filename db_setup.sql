CREATE DATABASE IF NOT EXISTS connect_test;
USE connect_test;

DROP TABLE IF EXISTS MetroPT;

CREATE TABLE MetroPT (
    timestamp TIMESTAMP,
    TP2 FLOAT,
    TP3 FLOAT,
    H1 FLOAT,
    DV_pressure FLOAT,
    Reservoirs FLOAT,
    Oil_temperature FLOAT,
    Motor_current FLOAT,
    COMP FLOAT,
    DV_eletric FLOAT,
    Towers FLOAT,
    MPG FLOAT,
    LPS FLOAT,
    Pressure_switch FLOAT,
    Oil_level FLOAT,
    Caudal_impulses FLOAT,
    Severity VARCHAR(255)
);