################
kafka-aggregator
################

A Kafka aggregator based on the `Faust <https://faust.readthedocs.io/en/latest/index.html>`_ Python Stream Processing library.

kafka-aggregator development is based on the `Safir <https://safir.lsst.io>`__ application template.


Overview
========

kafka-aggregator uses `Faust's windowing feature <https://faust.readthedocs.io/en/latest/userguide/tables.html#windowing>`_  to aggregate a stream of messages from Kafka.

kafka-aggregator implements a Faust agent, a "stream processor",  that adds messages from a source topic into a Faust table. The table is configured as a tumbling window with a size, representing the window duration (time interval) and an expiration time, which specifies the duration for which the data allocated to each window will be stored. Every time a window expires, a callback function is called to aggregate the messages allocated to that window. The size of the window controls the frequency of the aggregated stream.

kafka-aggregator uses `faust-avro <https://github.com/masterysystems/faust-avro>`_ to add Avro serialization and Schema Registry support to Faust. faust-avro can parse Faust models into Avro Schemas.

See `the docs <https://kafka-aggregator.lsst.io/>`_ for more information.
