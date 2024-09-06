# Kafka Connect Basic Transformation (SMT)

## Overview
This repository contains custom Kafka Connect Single Message Transform (SMT) implementation. SMTs are lightweight transformations applied to messages as they flow through Kafka Connect, typically used to modify records before they reach their destination.

## Prerequisites
* Apache Kafka and Kafka Connect running with **Java** 11+

## Installation
1. **Clone the repository**:
```bash
git clone https://github.com/wzzktndl/kafka-connect-basic-smt
cd kafka-connect-basic-smt
```
2. **Build the project**:
```bash
mvn clean package
```
3. **Add the SMT to your Kafka Connect plugin path**:
   Copy the resulting plugin directory from the `target/wzzktndl-kafka-connect-basicsmt-{version}.zip` to your Kafka Connect plugin path (Unzip to Connect plugin path). 
4. **Restart Kafka Connect service and look for logging like this**:
```shell
connect            | [2024-09-25 12:01:00,459] INFO Added plugin 'com.github.wzzktndl.kafka.connect.smt.ChangeCase' (org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader)
connect            | [2024-09-25 12:01:01,463] INFO Added alias 'Wrap' to plugin 'com.github.wzzktndl.kafka.connect.smt.ChangeCase' (org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader)
```

## Feature
| **Package** | **Type** | **Support Schemaless** | **Description**                                                                             |
|-------------|----------|------------------------|---------------------------------------------------------------------------------------------|
| ChangeCase  | Key      | No                     | Change case for specific field e.g. Uppercase or Lowercase, Working fine with nested field. |
| ChangeCase  | Value    | No                     | Change case for specific field e.g. Uppercase or Lowercase, Working fine with nested field.                                                    |

## ChangeCase
```Text
"transforms.ChangeCase.type": "com.github.wzzktndl.kafka.connect.smt.ChangeCase$Key"
"transforms.ChangeCase.type": "com.github.wzzktndl.kafka.connect.smt.ChangeCase$Value"
```
* `fields.list` : List of fields (String with comma separate).
* `case` : Case type (Uppercase or Lowercase).
### Usage
```text
"transforms.ChangeCase.type": "com.github.wzzktndl.kafka.connect.smt.ChangeCase$Value"
"transforms.ChangeCase.fields.list": "name,lastname,uppercase",
"transforms.ChangeCase.case": "Lowercase"
```
### Input
```json
{
  "name" : "NAME",
  "lastname" : "lastname",
  "objects" : {
     "uppercase" : "THIS IS UPPERCASE TEXT"
  }
}
```
### Output
```json
{
  "name" : "name",
  "lastname" : "lastname",
   "objects" : {
      "uppercase" : "this is uppercase text"
   }
}
```
---

## Roadmap
Upcoming feature are:
* **DropNullField** : Drop any null value from record.
* **TimeZoneConverter** : Change timezone for specific field in record.
* More and more

## License
This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

