# SABD2
Progetto 2 Sistemi e Architetture per Big Data

## Setup

Porsi nella folder  ```./cluster```  ed eseguire ```docker-compose build``` per effettuare la build e ```docker-compose up``` per instanziare il cluster.

Durante l'esecuzione sono presenti dei tempi di attesa per permettere la sincronizzazione tra i vari nodi del cluster.

A fine esecuzione è necessario ```docker-compose down``` per pulire l'ambienete ed eliminare i volumi.

## Link utili

Docker Compose per Kafka w/ ZooKeeper Bitnami
https://hub.docker.com/r/bitnami/kafka/


Docker Compose per Flink in Session Mode
 - https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/deployment/resource-providers/standalone/docker/
 
 ## Web UIs e Interafaccie


Conduktor (Client UI Kafka)
 - https://www.conduktor.io

Flink UI:
 - localhost:8081

Kafka Interfaccie Esterne:
- localhost:9093
- localhost:9094

Prometheus:
 - localhost:9090
 
 Grafana:
  - localhost:3000
  
  ## Framework, Tool e Librerie utilizzate
  
  - Flink
  - Kafka
  - ZooKeeper
  - Docker e Docker-Compose
  - Prometheus
  - Grafana
  - Conduktor
  - Java 1.8
  - Flink Kafka Connector
  
  # Risultati
  
  I risultati sono presenti nella folder Result in formato CSV
