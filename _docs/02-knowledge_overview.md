---
title: "Toolkit technical background overview"
permalink: /docs/knowledge/overview/
excerpt: "Basic knowledge of the toolkits technical domain."
last_modified_at: 2017-08-04T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "knowledgedocs"
---
{% include toc %}
{% include editme %}


With the increasing popularity of cloud-based solutions, the JSON format is becoming ever more ingrained in our applications. Take for example IBM Bluemix. Many of the services in Bluemix have REST APIs that use JSON as the common exchange format when sending or receiving data.

This toolkit provides a simple and straight-forward way to convert from SPL-to-JSON and JSON-to-SPL.

The toolkit contains only 2 operators, appropriately named: TupleToJSON and JSONToTuple. There are also native functions available to allow for converting tuples to JSON when using Custom operators.
