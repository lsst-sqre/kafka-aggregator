###########################
How to run kafka-aggregator
###########################


Running locally with docker-compose
===================================

In this guide, we use ``docker-compose`` to illustrate how to run kafka-aggregator. To run kafka-aggregator on a Kubernetes environment see the :ref:`installation` section instead.

kafka-aggregator `docker-compose configuration`_ run Confluent Kafka services.

.. Make a footnote ref to `this example`_.

.. _docker-compose configuration: https://github.com/lsst-sqre/kafka-aggregator/blob/master/docker-compose.yaml
.. _this example: https://github.com/confluentinc/examples/blob/5.3.2-post/cp-all-in-one/docker-compose.yml

Clone the kafka-aggregator repository:

.. code-block:: bash

  git clone https://github.com/lsst-sqre/kafka-aggregator.git

Start Zookeeper, a Broker, and the Confluent Schema Registry services:

.. code-block:: bash

  cd kafka-aggregator
  docker-compose up -d zookeeper broker schema-registry

Create a new Python virtual environment and install kafka-aggregator locally (kafka-aggregator has been tested with Python 3.9):

.. code-block:: bash

  python -m venv venv
  source venv/bin/activate
  make update


Initializing source topics
==========================

.. note::
  In practice kafka-aggregator expects that the source topics already exist in Kafka and that their Avro schemas are available from the Confluent Schema Registry. The instructions below are only necessary to run the kafka-aggregator example module.

Using the kafka-aggregator example module you can initialize source topics in Kafka, and produce messages for those topics.

With the default :ref:`configuration`, the following command initializes 10 source topics with 10 fields each.

.. code-block:: bash

  kafkaaggregator -l info init-example

You can check wether the source topics were created in kafka:

.. code-block:: bash

  docker-compose exec broker kafka-topics --bootstrap-server broker:29092 --list

and that the Avro schemas are available from the Confluent Schema Registry:

.. code-block:: bash

  curl http://localhost:8081/subjects


Generating Faust agents
=======================

Use this command to generate the Faust agents to process the source topics.

.. code-block:: bash

  kafkaaggregator -l info generate-agents

.. note::

  By default agents are generated under the ``./agents`` folder where kafka-aggregator runs.

For the source topics initialized with the kafka-aggregator example module you should have this output:

.. code-block:: bash

  kafkaaggregator -l info agents
  [2020-07-06 18:30:58,115] [54727] [INFO] [^Worker]: Starting...
  ┌Agents─────────────────────────────┬─────────────┬──────────────────────────────────────────────────────┐
  │ name                              │ topic       │ help                                                 │
  ├───────────────────────────────────┼─────────────┼──────────────────────────────────────────────────────┤
  │ @example-000.process_source_topic │ example-000 │ Process incoming messages for the example-000 topic. │
  │ @example-001.process_source_topic │ example-001 │ Process incoming messages for the example-001 topic. │
  │ @example-002.process_source_topic │ example-002 │ Process incoming messages for the example-002 topic. │
  │ @example-003.process_source_topic │ example-003 │ Process incoming messages for the example-003 topic. │
  │ @example-004.process_source_topic │ example-004 │ Process incoming messages for the example-004 topic. │
  │ @example-005.process_source_topic │ example-005 │ Process incoming messages for the example-005 topic. │
  │ @example-006.process_source_topic │ example-006 │ Process incoming messages for the example-006 topic. │
  │ @example-007.process_source_topic │ example-007 │ Process incoming messages for the example-007 topic. │
  │ @example-008.process_source_topic │ example-008 │ Process incoming messages for the example-008 topic. │
  │ @example-009.process_source_topic │ example-009 │ Process incoming messages for the example-009 topic. │
  └───────────────────────────────────┴─────────────┴──────────────────────────────────────────────────────┘
  [2020-07-06 18:30:58,153] [54727] [INFO] [^Worker]: Stopping...
  [2020-07-06 18:30:58,153] [54727] [INFO] [^Worker]: Gathering service tasks...
  [2020-07-06 18:30:58,153] [54727] [INFO] [^Worker]: Gathering all futures...
  [2020-07-06 18:30:59,156] [54727] [INFO] [^Worker]: Closing event loop


Starting a worker
=================

Use this command to start a kafka-aggregator worker:

.. code-block:: bash

  kafkaaggregator -l info worker


Producing messages
==================

On another terminal use this command to produce messages for the source topics. This command produces 6000 messages at 10Hz.

.. code-block:: bash

  kafkaaggregator -l info produce --frequency 10 --max-messages 6000

You can use `Confluent Control Center <http://localhost:9021>`_ to inspect the messages for the source and aggregation topics or use the following from the command line:

.. code-block:: bash

  docker-compose exec broker /bin/bash
  root@broker:/# kafka-console-consumer --bootstrap-server broker:9092 --topic example-000
  ...
  root@broker:/# kafka-console-consumer --bootstrap-server broker:9092 --topic example-000-aggregated


Inspecting the consumer lag
===========================

An important aspect to look at is the consumer lag for the ``kafkaaggregator`` consumers. An advantage of Faust is that you can easily add more workers to distribute the workload of the application. If the source topics are created with multiple partitions, individual partitions are assigned to different workers.


Internal vs. external managed topics
====================================

Faust manages topics declared as `internal` by the agents, like the aggregation topic, which is created by Faust and whose schema is also controlled by a Faust Record.

The kafka-aggregator example also demonstrates that we can aggregate source topics that are declared as `external`, i.e. not managed by Faust.  The agents assume that external topics exist and the messages can be deserialized using the Avro schemas, without specifying a model for the external topic in Faust.
