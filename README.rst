################
kafka-aggregator
################

A Kafka aggregator based on the `Faust <https://faust.readthedocs.io/en/latest/index.html>`_ Python Stream Processing library.

kafka-aggregator development is based on the `Safir <https://safir.lsst.io>`__ application template.


Faust
=====

We use the `Faust windowing <https://faust.readthedocs.io/en/latest/userguide/tables.html#windowing>`_ feature for aggregation of Kafka streams.

The general idea is that you can use a Faust agent to process the stream of events (messages) from a given Kafka topic and add those messages to a Faust table for persistence. The table is configured as a tumbling window. The window configuration parameters are the window size and the expiration time. When the window expires a callback function is called to process the messages in that window. That's when the aggregation happens and when a new Kafka topic is produced with the aggregated values.


Docker compose
==============

Docker compose makes it possible to run the ``kafkaaggregator`` application with a local Kafka cluster.  The ``docker-compose.yaml`` configuration includes services for Confluent Kafka (zookeeper, broker, schema-registry and control-center) based on `this example <https://github.com/confluentinc/examples/blob/5.3.1-post/cp-all-in-one/docker-compose.yml>`_.

Start the zookeeper, broker, schema-registry, internal-registry-url and control-center services:

.. code-block:: language

  docker-compose up zookeeper broker schema-registry internal-schema-registry control-center

you can check the status of the Kafka cluster opening the `Confluent Control Center <http://localhost:9021>`_ in the browser.

Installing kafkaaggregator
==========================

On another terminal session, create a new Python virtual environment and install the `kafkaaggregator` app:

.. code-block:: bash

  make update


The aggregation example
=======================

Before running the aggregation example you have to run this initialization command to create the example source topic in Kafka and to register its schema with the Schema registry.

.. code-block:: bash

  kafkaaggregator -l info init-example

You can check that the source topic was created in Kafka:

.. code-block:: bash

  docker-compose exec broker /bin/bash
  root@broker:/# kafka-topics --bootstrap-server broker:29092 --list


and that its schema was registered in the Schema Registry:

.. code-block:: bash

  curl http://localhost:8081/subjects

We plan on using the ``kafka-aggregator`` in a `multi-datacenter setup <https://docs.confluent.io/current/schema-registry/multidc.html>`_ where the aggregation happens in the destination cluster, so we don't want to replicate aggregation topic schemas to the source cluster Schema Registry.  To avoid collisions between schema IDs for schemas created at the source and destination clusters, we use an internal Schema Registry for the aggregation topic schemas which is configured to run on port ``28081``.

We use `faust-avro <https://github.com/masterysystems/faust-avro>`_ to add Avro serialization and schema registry support to Faust. It can parse Faust models into Avro Schemas.


Running the example
-------------------

Generate the Faust agents:

.. code-block:: bash

  kafkaaggregator -l info generate_agents

by default agents are generated under the ``./agents`` folder. And kafkaaggregator is configured to find them. For the ``kafkaaggregator-example`` topic you should have this output:

.. code-block:: bash

  kafkaaggregator -l info agents

  ┌Agents─────────────────────────────────────────┬─────────────────────────┬──────────────────────────────────────────────────────────────────┐
  │ name                                          │ topic                   │ help                                                             │
  ├───────────────────────────────────────────────┼─────────────────────────┼──────────────────────────────────────────────────────────────────┤
  │ @kafkaaggregator-example.process_source_topic │ kafkaaggregator-example │ Process incoming messages for the kafkaaggregator-example topic. │
  └───────────────────────────────────────────────┴─────────────────────────┴──────────────────────────────────────────────────────────────────┘


Start the ``kafkaaggregator`` worker:

.. code-block:: bash

  kafkaaggregator -l info worker

Run the following command in another terminal to produce messages for the source topic. In this example, it produces 6000 messages at 10Hz.

.. code-block::

  kafkaaggregator -l info produce --frequency 10 --max-messages 6000

You can use `Confluent Control Center <http://localhost:9021>`_ to inspect the messages for the source and aggregation topics, or using the command line:


.. code-block:: bash

  docker-compose exec broker /bin/bash
  root@broker:/# kafka-console-consumer --bootstrap-server broker:9092 --topic kafkaaggregator-src-topic
  ...
  root@broker:/# kafka-console-consumer --bootstrap-server broker:9092 --topic kafkaaggregator-agg-topic


An important aspect to look at is the lag for the ``kafkaaggregator`` consumers. An advantage of Faust is that you can easily add multiple workers to distribute the workload of the application. If topics are created with multiple partitions (see the ``config.topic_partitions`` configuration parameter) partitions are reassigned to different workers.

The following command starts a second ``kafkaaggregator`` worker on port ``6067``.

.. code-block:: bash

  kafkaaggregator -l info worker -p 6067


Internal vs. external managed topics
====================================

Faust manages topics declared as *internal* by the agents, like the aggregation topic, which is created by Faust and whose schema is also controlled by a Faust Record.

In real-life, source topics already exist in Kafka and their schemas are already registered in the Schema Registry. We demonstrate that we can run the aggregation example when a source topic is not managed by Faust, i.e the agents assume that the source topic exists and the messages can be deserialized without specifying a model for the source topic in Faust.
