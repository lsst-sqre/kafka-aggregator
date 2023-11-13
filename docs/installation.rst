.. _installation:

##################
Installation guide
##################

kafka-aggregator is meant to be run on Kubernetes and it assumes that Kafka is also running in the same kubernetes cluster. This section shows how to use a `Helm chart`_ to install kafka-aggregator. The main configuration settings you need to know to get it running are covered in the :ref:`configuration` section.


.. _`helm-chart`:

Helm chart
==========

There is a Helm chart for kafka-aggregator available from the `Rubin Observatory charts repository`_. To use the Helm chart, set the appropriate configuration values in the `values.yaml`_ file.

.. _Rubin Observatory charts repository: https://lsst-sqre.github.io/charts
.. _values.yaml: https://github.com/lsst-sqre/charts/blob/master/charts/kafka-aggregator/values.yaml


Argo CD
=======

kafka-aggregator is deployed using Argo CD. An example of Argo CD app using the Helm chart can be found `here <https://github.com/lsst-sqre/argocd-efd/tree/master/apps/kafka-aggregator>`_.
