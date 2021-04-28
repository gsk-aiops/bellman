---
layout: docs
title: Configuration
permalink: docs/configuration
---

# Configuration

The Bellman's Engine can receive some configuration flags that will affect the behaviour of it. In this section we will
explore what are the flags that we can currently setup, their description and their default values:

- **isDefaultGraphExclusive**:
    This flag will tell the engine in which way it will construct the default graph. There are two ways in which the
    Bellman's Engine can behave, **inclusive** or **exclusive**.
    
    The **inclusive** behaviour will add all the defined graphs in the Dataframe to the default graph. This means that
    on the queries there is no need to explicitly define in which graphs the query may apply by the use of the `FROM` 
    statement as all the graphs are included in the default graphs, so it will apply on all of them.
  
    The **exclusive** behaviour, unlike the inclusive, it won't add all the graphs defined in the Dataframe to the
    default graph. So to add a specific graph to the default graph we must explicitly tell the engine in the query
    by doing use of the `FROM` statement, on which graphs should the query apply.
  
    See [default graph demystified](https://blog.metaphacts.com/the-default-graph-demystified) for further explanation
    on inclusive/exclusive default graph.

    You must be aware that the selection of one the behaviours, specially the inclusive, could affect the expected 
    output of the query and also the performance of it.

    The default value of the flag is `true`, meaning that the default behaviour will be **exclusive**.
  

- **stripQuestionMarksOnOutput**:
    This flag will tell the Bellman's Engine to strip question marks of the Dataframe columns header if 'true' and
    it won't if `false`.
  
    The default value for this flag is `false`.
  

- **formatRdfOutput**:
    This flag will tell the Bellman's Engine whether it should apply implemented formatting to the output dataframe.
  
    The default value for this flag is `false`.