package com.gsk.kg.engine
package data


import com.gsk.kg.sparql.syntax.all._
import cats.instances.string._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import higherkindness.droste.data.Fix
import higherkindness.droste.Basis

class TreeRepSpec extends AnyFlatSpec with Matchers {

  "TreeRep.draw" should "generate a tree representation" in {

    TreeRep.Node(
      "1",
      Stream(
        TreeRep.Node(
          "2",
          Stream(
            TreeRep.Leaf("5")
          )
        ),
        TreeRep.Node(
          "3",
          Stream(
            TreeRep.Leaf("6")
          )
        ),
        TreeRep.Node(
          "4",
          Stream(
            TreeRep.Leaf("7")
          )
        )
      )
    ).drawTree.trim shouldEqual """
1
|
+- 2
|  |
|  `- 5
|
+- 3
|  |
|  `- 6
|
`- 4
   |
   `- 7""".trim()

  }

  it should "work as a typeclass for other types" in {
    import ToTree._
    import DAG._

    val dag = DAG.fromQuery.apply(sparql"""
      PREFIX  schema: <http://schema.org/>
      PREFIX  rdf:  <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX  xml:  <http://www.w3.org/XML/1998/namespace>
      PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>
      PREFIX  prism: <http://prismstandard.org/namespaces/basic/2.0/>
      PREFIX  litg:  <http://lit-search-api/graph/>
      PREFIX  litn:  <http://lit-search-api/node/>
      PREFIX  lite:  <http://lit-search-api/edge/>
      PREFIX  litp:  <http://lit-search-api/property/>

      CONSTRUCT {
        ?Document a litn:Document .
        ?Document litp:docID ?docid .
      }
      WHERE{
        ?d a dm:Document .
        BIND(STRAFTER(str(?d), "#") as ?docid) .
        BIND(URI(CONCAT("http://lit-search-api/node/doc#", ?docid)) as ?Document) .
      }
      """)

    dag.toTree.drawTree.trim shouldEqual """
Construct
|
+- BGP
|  |
|  +- Triple
|  |  |
|  |  +- ?Document
|  |  |
|  |  +- http://www.w3.org/1999/02/22-rdf-syntax-ns#type
|  |  |
|  |  `- http://lit-search-api/node/Document
|  |
|  `- Triple
|     |
|     +- ?Document
|     |
|     +- http://lit-search-api/property/docID
|     |
|     `- ?docid
|
`- Bind
   |
   +- VARIABLE(?Document)
   |
   +- URI
   |  |
   |  `- CONCAT
   |     |
   |     +- STRING(http://lit-search-api/node/doc#)
   |     |
   |     `- VARIABLE(?docid)
   |
   `- Bind
      |
      +- VARIABLE(?docid)
      |
      +- STRAFTER
      |  |
      |  +- STR
      |  |  |
      |  |  `- VARIABLE(?d)
      |  |
      |  `- #
      |
      `- BGP
         |
         `- Triple
            |
            +- ?d
            |
            +- http://www.w3.org/1999/02/22-rdf-syntax-ns#type
            |
            `- http://gsk-kg.rdip.gsk.com/dm/1.0/Document""".trim
  }

}
