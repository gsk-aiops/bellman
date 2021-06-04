package com.gsk.kg.engine

import com.gsk.kg.engine.QueryExtractor.QueryParam

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class QueryExtractorSpec extends AnyWordSpec with Matchers {
  
  "QueryExtractor" must {

    "extract info from the query" in {
      val query = """
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

      QueryExtractor.extractInfo(query)._2 shouldEqual Map(
        "http://gsk-kg.rdip.gsk.com/dm/1.0/kg" -> List(
          QueryParam("type","snapshot"),
          QueryParam("t","20200109")
        )
      )
    }

    "clean query from metadata in URIs" in {
      val query = """
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

      QueryExtractor.extractInfo(query)._1 shouldEqual """CONSTRUCT { ?te <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?docid . }
FROM <http://gsk-kg.rdip.gsk.com/dm/1.0/kg>
 WHERE { ?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://gsk-kg.rdip.gsk.com/dm/1.0/Document> . ?d <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?ds . ?ds <http://gsk-kg.rdip.gsk.com/dm/1.0/contains> ?te . BIND(STRAFTER(STR(?d), "#") as ?docid) }"""
    }
  }
}
