################
kafka-aggregator
################

A Kafka aggregator based on the `Faust <https://faust.readthedocs.io/en/latest/index.html>`_ Python Stream Processing library.

kafka-aggregator development is based on the `Safir <https://safir.lsst.io>`__ application template.


Docker compose
==============

Docker compose makes it possible to run the ``kafkaaggregator`` application with a local Kafka cluster.  The ``docker-compose.yaml`` configuration includes services for Confluent Kafka (zookeeper, broker, schema-registry and control-center) based on `this example <https://github.com/confluentinc/examples/blob/5.3.1-post/cp-all-in-one/docker-compose.yml>`_.


Start the zookeeper, broker, schema-registry, and control-center services:

.. code-block:: language

  docker-compose up zookeeper broker schema-registry internal-schema-registry control-center

you can check the status of the Kafka cluster opening the `Confluent Control Center <http://localhost:9021>`_ in the browser.

Installing kafkaaggregator
==========================

On another terminal session, create a new Python virtual environment and install the `kafkaaggregator` app:

.. code-block:: bash

  make update


Avro schemas
============

`faust-avro <https://github.com/masterysystems/faust-avro>`_ adds Avro serialization and schema registry support to Faust. It can parse Faust models into Avro Schemas, for example:

.. code-block:: bash

   # list the curret models
   kafkaaggregator models
   # dump the Avro schema for the corresponding model
   kafkaaggregator schema example.models.AggTopic

Before running the aggregation example you have to register the schemas for internal topics managed by Faust.

We plan on using the ``kafka-aggregator`` in a `multi-datacenter setup <https://docs.confluent.io/current/schema-registry/multidc.html>`_ where the aggregation happens in the destination cluster, so we don't want to replicate aggregated topic schemas to the source cluster Schema Registry.  To avoid collisions between schema IDs for schemas created at the source and destination clusters, we use an internal Schema Registry for the aggregated topic schemas which is configured to run on port ``28081``.

.. code-block:: bash

  kafkaaggregator register

You can verify that the schemas for the internal topics have been registered:

.. code-block:: bash

  curl http://localhost:28081/subjects

Internal vs. external managed topics
====================================

Faust manages topics declared as *internal* by the agents, like the aggregation topic, which is created by Faust and whose schema is also controlled by Faust.

In real-life, source topics already exist in Kafka and their schemas are already registered in the Schema Registry. We demonstrate that we can run the aggregation example when a source topic is not managed by Faust, i.e the agents assume that the source topic exists and the messages can be deserialized without specifying a model for the source topic in Faust.

However, to run the aggregation example, we have to initialize the source topic and that's done when we start the Faust worker.


Running the aggregation example
===============================

Start the ``kafkaaggregator`` worker:

.. code-block:: bash

  kafkaaggregator -l info worker

you can access the worker HTTP API locally on the default port ``6066``. In particular, the ``http://localhost:6066/count/`` endpoint reports the number of messages processed by the worker.

.. code-block:: bash

  curl http://localhost:6066/count/


The following command starts the producer for the source topic. In this example, it produces 6000 messages at 10Hz.

.. code-block:: bas

  kafkaaggregator -l info produce --frequency 10 --max-messages 6000

Using `Confluent Control Center <http://localhost:9021>`_, you can inspect the messages for the aggregation topic.

You can also inspect the lag for the ``kafkaaggregator`` consumers. An advantage of Faust is that you can easily add multiple workers to distribute the workload of the application. If topics are created with multiple partitions (see the ``config.topic_partitions`` configuration parameter) partitions are reassigned to different workers.

The following command starts a second ``kafkaaggregator`` worker on port ``6067``.

.. code-block:: bash

  kafkaaggregator -l info worker -p 6067
