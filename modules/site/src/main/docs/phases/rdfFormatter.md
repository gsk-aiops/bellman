---
layout: docs
title: RDF Formatter
permalink: docs/phases/rdfFormatter
---

## RDF Formatter

The RDF Formatter phase runs after the results have been received back from Spark, and transforms the dataset to adapt it to the constraints that RDF has on values.

It does some conversions, such as transforming numbers from their int representation `1` to their RDF one `"1"^^xsd:int`.