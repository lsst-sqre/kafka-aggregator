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

  docker-compose up zookeeper broker schema-registry control-center

you can check the status of the Kafka cluster opening the `Confluent Control Center <http://localhost:9021>`_ in your browser.

Installing `kafkaaggregator`
============================

On another terminal session, create a new Python virtual environment and install the `kafkaaggregator` app:

.. code-block:: bash

  make update


Avro schemas
============

`faust-avro <https://github.com/masterysystems/faust-avro>`_ adds Avro serialization and schema registry support to Faust. It can parse Faust Records into Avro Schemas,  for example:

.. code-block:: bash

   # list the curret models
   kafkaaggregator models
   # dump the Avro schema for the corresponding model
   kafkaaggregator schema testtopic.models.AggTestTopic

Before running the test topic aggregation example you have to register the schemas:

.. code-block:: bash

  kafkaaggregator register

You can verify that the schemas have been uploaded:

.. code-block:: bash

  curl http://localhost:8081/subjects

Running the test topic aggregation example
==========================================

Start the ``kafkaaggregator`` worker:

.. code-block:: bash

  kafkaaggregator -l info worker

you can access the worker HTTP API locally on the default port ``6066``. In particular, the ``http://localhost:6066/count/`` endpoint reports the number of messages processed by the worker.

.. code-block:: bash

  curl http://localhost:6066/count/


The following command starts the ``kafkaaggregator`` producer for the test topic. In this example it produces 6000 messages at 10Hz.

.. code-block:: bash

  kafkaaggregator -l info produce --frequency 10 --max-messages 6000

Using `Confluent Control Center <http://localhost:9021>`_, you can inspect the messages for the aggregated topic ``agg-test-topic``.

You can also inspect the lag for the ``kafkaaggregator`` consumers. An advantage of Faust is that you can easily add multiple workers to distribute the workload of the application. If topics are created with multiple partitions (see the ``config.topic_partitions`` configuration parameter) partitions are reassigned to different workers.

The following command starts a second ``kafkaaggregator`` worker on port ``6067``.

.. code-block:: bash

  kafkaaggregator -l info worker -p 6067
