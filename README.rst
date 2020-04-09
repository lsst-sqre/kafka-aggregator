################
kafka-aggregator
################

A Kafka aggregator based on the `Faust <https://faust.readthedocs.io/en/latest/index.html>`_ Python Stream Processing library.

kafka-aggregator development is based on the `Safir <https://safir.lsst.io>`__ application template.


Running the test topic aggregation example
==========================================

Docker compose makes it possible to run the ``kafkaaggregator`` application with a local Kafka cluster.  The ``docker-compose.yaml`` configuration file includes services for the application itself and for Confluent Kafka (zookeeper, broker, schema-registry and control-center), based on `this example <https://github.com/confluentinc/examples/blob/5.3.1-post/cp-all-in-one/docker-compose.yml>`_.

Build an image for the ``kafkaaggregator`` app:

.. code-block:: bash

  docker-compose build

Start zookeeper, broker, schema-registry, and control-center services:

.. code-block:: language

  docker-compose up zookeeper broker schema-registry control-center

you can check the status of the Kafka cluster using the Confluent Control Center at http://localhost:9021.

On another terminal session start a ``kafkaaggregator`` worker:

.. code-block:: bash

  docker-compose run kafkaaggregator -l info worker -p 6066

you can access this worker at http://localhost:6066, ports ``[6066-6069]`` are exposed in the docker-compose configuration.

The following command starts the ``kafkaaggregator`` producer for the test topic. In this example it procduces 6000 messages at 10Hz.

.. code-block:: bash

  docker-compose run kafkaaggregator -l info produce --frequency 10 --max-messages 6000

Using the Confluent Control Center, you can inspect the messages for the aggregated test topic ``agg-test-topic``.

You can also inspect the lag for the ``kafkaaggregator`` consumers. An advantage of Faust is that you can easily add multiple workers to distribute the workload of the application. If topics are created with multiple partitions (see the ``config.topic_partitions`` configuration parameter) partitions are reassigned to different workers.

The following command starts a second ``kafkaaggregator`` worker on port 6067.

.. code-block:: bash

  docker-compose run kafkaaggregator worker -l info -p 6067
