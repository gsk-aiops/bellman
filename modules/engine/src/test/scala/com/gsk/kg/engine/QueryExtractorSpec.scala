package com.gsk.kg.engine

import com.gsk.kg.engine.QueryExtractor.QueryParam

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class QueryExtractorSpec extends AnyWordSpec with Matchers {

  "QueryExtractor" must {

    "get info and clean query with GRAPH statements" in {
      val query = """
        PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>
        CONSTRUCT {
          ?te dm:contains ?docid .
        }
        WHERE {
           GRAPH <http://gsk-kg.rdip.gsk.com/dm/1.0/Semmed?type=snapshot&t=20200309>
           {
             ?ds dm:contains ?te .
           }
           GRAPH <http://gsk-kg.rdip.gsk.com/dm/1.0/Elsevier?type=incremental&t=20200109>
           {
             ?d a dm:Document .
             ?d dm:contains ?ds .
           }
           BIND(STRAFTER(str(?d), "#") as ?docid) .
        }"""

      val result = QueryExtractor.extractInfo(query)

      result._2 shouldEqual Map(
        "http://gsk-kg.rdip.gsk.com/dm/1.0/Elsevier" ->
          List(QueryParam("type", "incremental"), QueryParam("t", "20200109")),
        "http://gsk-kg.rdip.gsk.com/dm/1.0/Semmed" ->
          List(QueryParam("type", "snapshot"), QueryParam("t", "20200309"))
      )

      result._1 shouldEqual """CONSTRUCT { ?te <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?docid . }

 WHERE { { GRAPH <http://gsk-kg.rdip.gsk.com/dm/1.0/Semmed> { ?ds <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?te . } } JOIN { GRAPH <http://gsk-kg.rdip.gsk.com/dm/1.0/Elsevier> { ?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://gsk-kg.rdip.gsk.com/dm/1.0/Document> . ?d <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?ds . } } BIND(STRAFTER(STR(?d), "#") as ?docid) }"""
    }

    "extract info from FROM statement" in {
      val query  = """
PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>
CONSTRUCT {
  ?te dm:contains ?docid .
}
FROM <http://gsk-kg.rdip.gsk.com/dm/1.0/ccc?type=snapshot&t=20200109>
WHERE {
   ?d a dm:Document .
   ?d dm:contains ?ds .
   ?ds dm:contains ?te .
   BIND(STRAFTER(str(?d), "#") as ?docid) .
}
    """
      val result = QueryExtractor.extractInfo(query)

      result._2 shouldEqual Map(
        "http://gsk-kg.rdip.gsk.com/dm/1.0/ccc" -> List(
          QueryParam("type", "snapshot"),
          QueryParam("t", "20200109")
        )
      )

      result._1 shouldEqual """CONSTRUCT { ?te <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?docid . }
FROM <http://gsk-kg.rdip.gsk.com/dm/1.0/ccc>
 WHERE { ?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://gsk-kg.rdip.gsk.com/dm/1.0/Document> . ?d <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?ds . ?ds <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?te . BIND(STRAFTER(STR(?d), "#") as ?docid) }"""
    }

    "format knowledge graph URI correctly" in {
      val query  = """
PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>
CONSTRUCT {
  ?te dm:contains ?docid .
}
FROM <http://gsk-kg.rdip.gsk.com/dm/1.0/kg?type=snapshot&t=20200109>
WHERE {
   ?d a dm:Document .
   ?d dm:contains ?ds .
   ?ds dm:contains ?te .
   BIND(STRAFTER(str(?d), "#") as ?docid) .
}
    """
      val result = QueryExtractor.extractInfo(query)

      result._2 shouldEqual Map(
        "" -> List(
          QueryParam("type", "snapshot"),
          QueryParam("t", "20200109")
        )
      )

      result._1 shouldEqual """CONSTRUCT { ?te <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?docid . }

 WHERE { ?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://gsk-kg.rdip.gsk.com/dm/1.0/Document> . ?d <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?ds . ?ds <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?te . BIND(STRAFTER(STR(?d), "#") as ?docid) }"""
    }

    "work correctly when more than one FROM statement appears" in {
      val query  = """
PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>
CONSTRUCT {
  ?te dm:contains ?docid .
}
FROM <http://gsk-kg.rdip.gsk.com/dm/1.0/ccc?type=snapshot&t=20200109>
FROM <http://gsk-kg.rdip.gsk.com/dm/1.0/semmed?type=snapshot&t=20200109>
WHERE {
   ?d a dm:Document .
   ?d dm:contains ?ds .
   ?ds dm:contains ?te .
   BIND(STRAFTER(str(?d), "#") as ?docid) .
}
    """
      val result = QueryExtractor.extractInfo(query)

      result._2 shouldEqual Map(
        "http://gsk-kg.rdip.gsk.com/dm/1.0/semmed" -> List(
          QueryParam("type", "snapshot"),
          QueryParam("t", "20200109")
        ),
        "http://gsk-kg.rdip.gsk.com/dm/1.0/ccc" -> List(
          QueryParam("type", "snapshot"),
          QueryParam("t", "20200109")
        )
      )

      result._1 shouldEqual """CONSTRUCT { ?te <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?docid . }
FROM <http://gsk-kg.rdip.gsk.com/dm/1.0/ccc>
FROM <http://gsk-kg.rdip.gsk.com/dm/1.0/semmed>
 WHERE { ?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://gsk-kg.rdip.gsk.com/dm/1.0/Document> . ?d <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?ds . ?ds <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?te . BIND(STRAFTER(STR(?d), "#") as ?docid) }"""
    }

    "handle GRAPH unions correctly" in {
      val query = """
        PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>
        CONSTRUCT {
          ?te dm:contains ?docid .
        }
        WHERE {
           { GRAPH <http://gsk-kg.rdip.gsk.com/dm/1.0/Semmed?type=snapshot&t=20200309>
             {
               ?ds dm:contains ?te .
             } 
           } UNION {
             GRAPH <http://gsk-kg.rdip.gsk.com/dm/1.0/Elsevier?type=incremental&t=20200109>
             {
               ?d a dm:Document .
               ?d dm:contains ?ds .
             }
           }
           BIND(STRAFTER(str(?d), "#") as ?docid) .
        }"""

      val result = QueryExtractor.extractInfo(query)

      result._2 shouldEqual Map(
        "http://gsk-kg.rdip.gsk.com/dm/1.0/Elsevier" ->
          List(QueryParam("type", "incremental"), QueryParam("t", "20200109")),
        "http://gsk-kg.rdip.gsk.com/dm/1.0/Semmed" ->
          List(QueryParam("type", "snapshot"), QueryParam("t", "20200309"))
      )

      result._1 shouldEqual """CONSTRUCT { ?te <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?docid . }

 WHERE { { GRAPH <http://gsk-kg.rdip.gsk.com/dm/1.0/Semmed> { ?ds <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?te . } } UNION { GRAPH <http://gsk-kg.rdip.gsk.com/dm/1.0/Elsevier> { ?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://gsk-kg.rdip.gsk.com/dm/1.0/Document> . ?d <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?ds . } } BIND(STRAFTER(STR(?d), "#") as ?docid) }"""
    }
  }
}
