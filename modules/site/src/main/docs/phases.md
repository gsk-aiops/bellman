---
layout: docs
title: Phases
permalink: docs/phases
---

# Phases

Bellman is architected as a nanopass compiler, using Recursion Schemes as a framework.  The main phases are the following:

- [**parser**](parser) transforms query strings to a Query ADT
- [**transformToGraph**](transformToGraph) creates the DAG we handle internally in Bellman
- [**optimizer**](optimizer) runs optimizations on the DAG
- [**staticAnalysis**](staticAnalysis) performs some analysis on the query to reject bad queries
- [**engine**](engine) actually runs the query in Spark
- [**rdfFormatter**](rdfFormatter) converts the result to a DataFrame with RDF formatted values 