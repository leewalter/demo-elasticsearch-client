# Demo of the High-Level REST Client for Elasticsearch

This repository contains a demo project of how to use
The Elasticsearch High-Level REST Client to index and query
a sample dataset.

## About The Data

The dataset includes trip data from the SF bike-share program. It includes 192082
rides.

for more information, check out [Ford GoBike Trip Data](https://www.fordgobike.com/system-data).

## How To Run

### 1. Run Elasticsearch & Kibana

find the download links to 7.0-beta1 here!

- [Elasticsearch 7.0.0-beta1](https://www.elastic.co/downloads/elasticsearch#preview-release)
- [Kibana 7.0.0-beta1](https://www.elastic.co/downloads/kibana#preview-release)

### 2. Run Demo
Running the demo will download the Ford GoBike SF dataset and
begin indexing against a locally running Elasticsearch cluster.

```bash
$ ./gradlew run
```

### 3. Visualize in Kibana
