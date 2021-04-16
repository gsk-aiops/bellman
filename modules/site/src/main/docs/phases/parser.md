---
layout: docs
title: Parser
permalink: docs/phases/parser
---

## Parser

Parser transforms the String representing the query into the `Expr` AST.  This Expr AST is the representation of the SparQL algebra.

The process of converting the string to our AST is done using Apache Jena.  We parse intro Jena Algebra, transform that to the _lisp like_ string that Jena uses for the algebra, and then parse that to Scala datatypes.