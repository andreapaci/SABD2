# SABD2
Progetto 2 Sistemi e Architetture per Big Data


## Link utili

Docker Compose per Kafka w/ ZooKeeper Confluent
https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html


Docker Compose per Kafka w/ ZooKeeper Bitnami
https://hub.docker.com/r/bitnami/kafka/


Docker Compose per Flink in Session Mode
https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/deployment/resource-providers/standalone/docker/


Conduktor (Client UI Kafka)
https://www.conduktor.io

Flink UI:
localhost:8081

Kafka Interfaccie Esterne:
- localhost:9093
- localhost:9094

## Producer

Producer Single Thread che fa
- Parsing del dataset e Load in memoria (Non ottimale)
- Sorting del CSV
- Producer di messaggi per Kafka
- Commit topology a Flink
- Legge outout


## TODO

## SVILUPPI FUTURI

- Aggiungere finestre con sottofinestre per ottimizzare la computazione (invece di fare 4 finestre da 7 giorni e una da 28 separate, inglobare le 4 in 28 per ottimizzare computazione)

## Note

- No necessità di eliminare il messaggio alla lettura, kafka ha un retention di 7 giorni in automatico
- Fare suddivisione mare correttamente
- Automatizzare conversione celle in lettere
- Usato offset invece che finestra allineata al primo valore per evitare tuple fuori ordine
