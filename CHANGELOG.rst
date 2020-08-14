##########
Change log
##########

0.2.0 (2020-08-14)
==================

* Add first and third quartiles (``q1`` and ``q3``) to the list of summary statistics computed by the aggregator.
* Ability to configure the list of summary statistics to be computed.
* Pinned top-level requeriments.
* Add Kafka Connect to the docker-compose setup.
* Use only one Schema Registry by default to simplify local execution.
* First release to PyPI.


0.1.0 (2020-07-13)
==================

Initial release of kafka-aggregator with the following features:

* Use Faust windowing feature to aggregate a stream of messages.
* Use Faust-avro to add Avro serialization and Schema Registry support to Faust.
* Support to an internal Schema Registry to store schemas for the aggreated topics (optional).
* Create aggregation topic schemas from the source topic schemas and from the list of summary statistics to be computed.
* Ability to create Faust records dynamically from aggregation topic schemas.
* Ability to auto-generate code for the Faust agents (stream processors).
* Compute summary statistics for numeric fields: ``min()``, ``mean()``, ``median()``, ``stdev()``, ``max()``.
* Add example module to initialize a number of source topics in kafka, control the number of fields in each topic, and produce messages for those topics at a given frequency.
* Use Kafdrop to inspect messages from source and aggregated topics.
* Add kafka-aggregator documentation site.
