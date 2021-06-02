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

  }

}
