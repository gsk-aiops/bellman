---
layout: docs
title: Static Analysis
permalink: docs/phases/staticAnalysis
---

## Static Analysis

We perform some static analysis on the query in order to reject invalid queries before sending them to the Spark cluster.

Currently we check that the variables used in the query are bound, but more analysis may come in the future.