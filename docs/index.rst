################
Kafka-aggregator
################

A Kafka aggregator based on the `Faust <https://faust.readthedocs.io/en/latest/index.html>`_ Python Stream Processing library.

This site provides documentation for kafka-aggregator's installation, configuration, user and development guides, and API reference. Before installing kafka-aggregator, you might want to use the docker-compose set up as a way to run it locally, in this case jump straight to the `Configuration`_ and `User guide`_ sessions.

Overview
========

kafka-aggregator uses `Faust's windowing feature <https://faust.readthedocs.io/en/latest/userguide/tables.html#windowing>`_  to aggregate a stream of messages from Kafka.

kafka-aggregator implements a Faust agent, a "stream processor",  that adds messages from a source topic into a Faust table. The table is configured as a tumbling window with a size, representing the window duration (time interval) and an expiration time, which specifies the duration for which the data allocated to each window will be stored. Every time a window expires, a callback function is called to aggregate the messages allocated to that window. The size of the window controls the frequency of the aggregated stream.

kafka-aggregator uses `faust-avro <https://github.com/masterysystems/faust-avro>`_ to add Avro serialization and Schema Registry support to Faust. faust-avro can parse Faust models into Avro Schemas.

To accomplish that, kafka-aggregator also needs to:

1. Retrieve the Avro schema for a source topic from the Schema Registry;
2. Create the aggregation fields based on the source topic schema and the summary statistics to be computed;
3. Create a Faust Record class at run-time representing the aggregation topic;
4. Use faust-avro to parse the Faust Record and create the Avro schema for the aggregation topic.
5. Register the Avro schema for the aggregation topic with the Schema Registry;
6. Generate static code for the Faust agent to process the source topic stream.
7. Start a Faust worker to run the Faust agent.
8. Compute summary statistics for the messages in the aggregation window and produce the aggregated message.

Summary statistics
------------------
kafka-aggregator uses the `Python statistics`_ module to compute summary statistics for each field in the messages allocated to an aggregation window.

.. table:: *Summary statistics computed by kafka-aggregator*.

  +----------+--------------------------------------+
  | mean()   | Arithmetic mean ("average") of data. |
  +----------+--------------------------------------+
  | median() | Median (middle value) of data.       |
  +----------+--------------------------------------+
  | min()    | Minimum value of data.               |
  +----------+--------------------------------------+
  | max()    | Maximum value of data.               |
  +----------+--------------------------------------+
  | stdev()  | Sample standard deviation of data.   |
  +----------+--------------------------------------+

.. _Python statistics: https://docs.python.org/3/library/statistics.html


Scalability
-----------

As a kafka application, it is easy to scale kafka-aggregator horizontally by increasing the number of partitions for the source topics and by running more workers.

To help to define the number of workers in a given environment, kafka-aggregator comes with an example module. Using the kafka-aggregator example module, you can initialize a number of source topics in kafka, control the number of fields in each topic, and produce messages for those topics at a given frequency. It is a good way to start running kafka-aggregator and to understand how it scales in a particular environment.



Installation
============

.. toctree::
   :maxdepth: 2

   installation

Configuration
=============

.. toctree::
   :maxdepth: 2

   configuration

User guide
==========

.. toctree::
   :maxdepth: 2

   userguide

Development guide
=================

.. toctree::
   :maxdepth: 2

   development
   release

API
===

.. toctree::
   :maxdepth: 2

   api


Project information
===================

The GitHub repository for `kafka-aggregator` is https://github.com/lsst-sqre/kafka-aggregator

.. toctree::
   :maxdepth: 2

   contributing
   changelog

See the LICENSE_ file for licensing information.

.. _LICENSE: https://github.com/lsst-sqre/kafka-aggregator/blob/master/LICENSE
