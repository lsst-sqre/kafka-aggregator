################
Kafka-aggregator
################

A Kafka aggregator based on the `Faust <https://faust.readthedocs.io/en/latest/index.html>`_ Python Stream Processing library.

This site provides documentation for the kafka-aggregator installation, configuration, user and development guides, and API reference.

Before installing kafka-aggregator, you might want to use the docker-compose set up as a way to run it locally, in this case jump straight to the `Configuration`_ and `User guide`_ sessions.

Overview
========

kafka-aggregator uses `Faust's windowing feature <https://faust.readthedocs.io/en/latest/userguide/tables.html#windowing>`_  to aggregate Kafka streams.

kafka-aggregator implements a Faust agent (stream processor) that adds messages from a source topic into a Faust table. The table is configured as a tumbling window with a size and an expiration time. Every time a window expires, a callback function is called to aggregate the messages allocated to that window. The size of the window controls the frequency of the aggregated stream.

kafka-aggregator uses `faust-avro <https://github.com/masterysystems/faust-avro>`_ to add Avro serialization and Schema Registry support to Faust.


.. figure:: /_static/kafka-aggregator.svg
   :name: Kafka-aggretor architecture diagram

   Figure 1. Kafka-aggregator architecture diagram showing Kafka and Faust components.

Summary statistics
------------------
kafka-aggregator uses the `Python statistics`_ module to compute summary statistics for each numerical field in the source topic.

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
  | q1()     | First quartile of the data.          |
  +----------+--------------------------------------+
  | q3()     | Third quartile of the data.          |
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
