.. _configuration:

######################
Configuration settings
######################

In this section we discuss the main configuration settings to get kafka-aggregator running. The `Configuration class`_ is also documented here and can be used as reference for the configuration settings exposed in the `values.yaml`_ when using the :ref:`helm-chart`.

.. _values.yaml: https://github.com/lsst-sqre/charts/blob/master/charts/kafka-aggregator/values.yaml



Kafka-aggregator configuration
==============================

To configure kafka-aggretor with Kafka, you need to provide the Kafka `broker` and the `schema_registry_url` URLs.

The `Confluent Schema Registry`_ is used to manage Avro schemas for the source and aggregation topics.

In a Kafka multi-site set up, usually there's `continuous migration`_ of the Avro schemas from the source Schema Registry to the destination Schema Registry. kafka-aggregator normally runs on the destination cluster and thus it would register the aggregation topic schemas to the destination Schema Registry. Depending on how replication is configured, you don't have schema migration back to the source cluster or you might not want to replicate the schemas for the aggregation topics. Either way, to avoid collisions between schema IDs for schemas created at the source Schema Registry and destination Schema Registry, we recommend deploying a separate Schema Registry to store the schemas for the aggregation topics. In this case, set the `internal_registry_url` configuration accordingly. The `docker-compose`_ configuration in the kafka-aggregator repository shows how to configure an internal schema registry for kafka-aggregator.


.. _Confluent Schema Registry: https://docs.confluent.io/current/schema-registry/index.html
.. _continuous migration: https://docs.confluent.io/current/schema-registry/installation/migrate.html#continuous-migration
.. _docker-compose: https://github.com/lsst-sqre/kafka-aggregator/blob/master/docker-compose.yaml


The following configuration settings are specific to the kafka-aggregator application.

kafka-aggregator selects source topics from Kafka using a regular expression `topic_regex` and can exclude source topics listed in the `excluded_topics` list.

Configure the aggregation window size (time interval) using `window_size`, and the `window_expires`, which specifies the duration for which to store the data allocated to each window.

.. note::

  Faust allocates at least one message on each aggregation window, if `window_size` is smaller than the time interval between two consecutive messages Faust will skip that window and no aggregation is computed.


Make sure `window_size` is large enough to minimize the standard error associated to the number of messages (sample size) allocated in a window . The smaller the standard error the more precise the computed estimate is. kafka-aggregator stores the sample size in the ``count`` field of each aggregated message. That can be used, for example, to compute the standard error of the mean, given by ``SE=stdev/sqrt(count)`` where ``stdev`` is the sample standard deviation (the square root of the sample variance) computed and stored for each field in the aggregated message.

kafka-aggregator allows to control the minimum sample size to compute statistics by setting the  `min_sample_size` parameter, which by default is `min_sample_size=2`.

.. note::

  If ``count`` is less than `min_sample_size` there are not enough values in the aggregation window to compute statistics, then kafka-aggregator uses the first value in the window instead.

It is also possible to exclude specific fields from aggregation using the `excluded_field_names` list.


.. note::

  kafka-aggregator excludes by default the field names ``time``, ``window_size`` and ``count``. In particular, those fields are added to each aggregated message: ``time`` is the midpoint of the aggregation window, ``window_size`` is the size of the aggregation window and ``count`` is the sample size as discussed above.

By default aggregation topic are named like ``{source_topic_name}-aggregated``. That can be changed by setting the `topic_rename_format` parameter in the configuration.


Example module configuration
============================

The kafka-aggregator example module can be used to initialize "example source topics" in Kafka and produce messages for those topics.

`source_topic_name_prefix` sets the name prefix for the example source topics. The number of topics to create is set by `ntopics` and the number of fields in each topic is set by `nfields`. The number of partitions for the example source topics is set by `topic_partitions`.

The example module also produces messages for the example source topics. The frequency in Hz in which messages are produced is set by `frequency` and the maximum number of messages produced for each topic is set by `max_messages`. If `max_messages` is a number smaller than 1, an indefinite number of messages is produced.


Configuration class
===================

.. currentmodule:: kafkaaggregator.config

.. autoclass:: Configuration
   :show-inheritance:

   .. rubric:: Attributes Summary

   .. autosummary::

      ~Configuration.agent_template_file
      ~Configuration.agents_output_dir
      ~Configuration.broker
      ~Configuration.frequency
      ~Configuration.internal_registry_url
      ~Configuration.max_messages
      ~Configuration.min_sample_size
      ~Configuration.nfields
      ~Configuration.ntopics
      ~Configuration.registry_url
      ~Configuration.source_topic_name_prefix
      ~Configuration.store
      ~Configuration.topic_partitions
      ~Configuration.topic_regex
      ~Configuration.topic_rename_format
      ~Configuration.window_expires
      ~Configuration.window_size

   .. rubric:: Attributes Documentation

   .. autoattribute:: agent_template_file
   .. autoattribute:: agents_output_dir
   .. autoattribute:: broker
   .. autoattribute:: frequency
   .. autoattribute:: internal_registry_url
   .. autoattribute:: max_messages
   .. autoattribute:: min_sample_size
   .. autoattribute:: nfields
   .. autoattribute:: ntopics
   .. autoattribute:: registry_url
   .. autoattribute:: source_topic_name_prefix
   .. autoattribute:: store
   .. autoattribute:: topic_partitions
   .. autoattribute:: topic_regex
   .. autoattribute:: topic_rename_format
   .. autoattribute:: window_expires
   .. autoattribute:: window_size
