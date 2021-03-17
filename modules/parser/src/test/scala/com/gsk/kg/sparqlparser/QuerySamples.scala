package com.gsk.kg.sparqlparser

object QuerySamples {

  // Get a small sample of documents from the KG
  val q1 = """
    SELECT ?docid ?doi WHERE {
        ?docid a <http://gsk-kg.rdip.gsk.com/dm/1.0/Document> .
        ?docid <http://prismstandard.org/namespaces/basic/2.0/doi> ?doi .
    } LIMIT 20
    """

  // Find the label for an ontology term (NICR2579 cell)
  val q2 = """
    SELECT ?label WHERE {
        <http://purl.obolibrary.org/obo/CLO_0052588>
        <http://www.w3.org/2000/01/rdf-schema#label>
        ?label.
    }
    """

  // Find the distinct labels for an ontology term (NICR2579 cell)
  // CLO terms will generally only be in CLO, this should be the same as above
  val q3 = """
    SELECT DISTINCT ?label WHERE {
        <http://purl.obolibrary.org/obo/CLO_0052588>
        <http://www.w3.org/2000/01/rdf-schema#label>
        ?label.
    }
    """

  // Get all relations of NICR2579 cell
  // None of these should be blank nodes
  val q4 = "SELECT ?p ?o WHERE { <http://purl.obolibrary.org/obo/CLO_0052588> ?p ?o .}"

  // Get parent classes of a class of cell lines
  // This includes a distinct IRI and a BNode
  val q5 = """
    SELECT ?parent WHERE {
        <http://purl.obolibrary.org/obo/CLO_0037232>
        <http://www.w3.org/2000/01/rdf-schema#subClassOf>
        ?parent.
    }
    """

  // Get parent classes of a class of cell lines
  // Filter out BNode result
  val q6 = """
    SELECT ?parent WHERE {
        {
            <http://purl.obolibrary.org/obo/CLO_0037232>
            <http://www.w3.org/2000/01/rdf-schema#subClassOf>
            ?parent .
        }
        FILTER (!isBlank(?parent))
    }
    """

  // Get species source of a cell line
  // Tests multiple hops
  val q7 = """
    SELECT ?species WHERE {
        <http://purl.obolibrary.org/obo/CLO_0037232> <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?derived_node .
        ?derived_node a <http://www.w3.org/2002/07/owl#Restriction>;
            <http://www.w3.org/2002/07/owl#onProperty> <http://purl.obolibrary.org/obo/RO_0001000>;
            <http://www.w3.org/2002/07/owl#someValuesFrom> ?species_node .
        ?species_node a <http://www.w3.org/2002/07/owl#Restriction>;
            <http://www.w3.org/2002/07/owl#onProperty> <http://purl.obolibrary.org/obo/BFO_0000050>;
            <http://www.w3.org/2002/07/owl#someValuesFrom> ?species .
    }
    """

  // Get species source of a cell line
  // Prefixed to reduce query length
  // Tests multiple hops and prefixes
  val q8 = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX obo-term: <http://purl.obolibrary.org/obo/>
    SELECT ?species WHERE {
        obo-term:CLO_0037232 rdfs:subClassOf ?derived_node .
        ?derived_node a owl:Restriction;
            owl:onProperty obo-term:RO_0001000;
            owl:someValuesFrom ?species_node .
        ?species_node a owl:Restriction;
            owl:onProperty obo-term:BFO_0000050;
            owl:someValuesFrom ?species .
    }
    """

  // Find the label for an ontology term (respiration organ)
  val q9 = """
    SELECT ?label WHERE {
        <http://purl.obolibrary.org/obo/UBERON_0000171>
        <http://www.w3.org/2000/01/rdf-schema#label>
        ?label .
    }
    """

  // Get parent classes of respiration organ
  val q10 = """
    SELECT ?parent WHERE {
        <http://purl.obolibrary.org/obo/UBERON_0000171>
        <http://www.w3.org/2000/01/rdf-schema#subClassOf>
        ?parent .
    }
    """

  // Get the label for the parent of respiration organ (organ)
  // Tests multiple hops and DISTINCT
  val q11 = """
    SELECT DISTINCT ?parent_name WHERE {
        <http://purl.obolibrary.org/obo/UBERON_0000171>
        <http://www.w3.org/2000/01/rdf-schema#subClassOf>
        ?parent .
        ?parent <http://www.w3.org/2000/01/rdf-schema#label> ?parent_name .
    }
    """

  // Get children of lung, returning lung in the output
  // Tests BIND
  val q12 =  """
    SELECT ?s ?p ?o WHERE {
        ?s ?p <http://purl.obolibrary.org/obo/UBERON_0002048> .
        FILTER (!isBlank(?s)) .
        BIND (<http://purl.obolibrary.org/obo/UBERON_0002048> AS ?o)
    }
    """

  // Test BIND in another position in the query
  // Result should be the same as previous
  val q13 = """
    SELECT ?s ?p ?o WHERE {
        BIND (<http://purl.obolibrary.org/obo/UBERON_0002048> AS ?o)
        ?s ?p <http://purl.obolibrary.org/obo/UBERON_0002048> .
        FILTER (!isBlank(?s)) .
    }
    """

  // Get triples referring to lung either as subject or object
  // Test UNION
  val q14 = """
    SELECT ?s ?p ?o WHERE {
        {
            ?s ?p <http://purl.obolibrary.org/obo/UBERON_0002048> .
            FILTER (!isBlank(?s)) .
            BIND (<http://purl.obolibrary.org/obo/UBERON_0002048> AS ?o)
        } UNION {
            <http://purl.obolibrary.org/obo/UBERON_0002048> ?p ?o .
            FILTER (!isBlank(?o)) .
            BIND (<http://purl.obolibrary.org/obo/UBERON_0002048> AS ?s)
        }
    }
    """

  // Describe lung
  val q15 = "DESCRIBE <http://purl.obolibrary.org/obo/UBERON_0002048>"

  // Describe the non-blank children of lung
  val q16 = """
    DESCRIBE ?s WHERE {
        ?s <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://purl.obolibrary.org/obo/UBERON_0002048> .
        FILTER (!isBlank(?s)) .
    }
    """

  // Concept mapping query - gets info on the UMLS concept links
  // Tests str conversion and logical operators
  val q17 =  """
    SELECT ?s ?o WHERE {
        ?s a <http://gsk-kg.rdip.gsk.com/dm/1.0/UMLSConcept> .
        ?s <http://gsk-kg.rdip.gsk.com/dm/1.0/conceptLink> ?o .
        FILTER (regex(str(?o),"https://meshb.nlm.nih.gov/record/ui")
            || regex(str(?o),"https://www.ncbi.nlm.nih.gov/taxonomy/")
            || regex(str(?o),"https://www.ensembl.org/Homo_sapiens/Gene/Summary?g=")
        ) .
    }
    """

  // Larger union with filter
  // Tests FILTER positioning with graph sub-patterns
  val q18 = """
    SELECT ?s ?o WHERE {
        ?s a <http://gsk-kg.rdip.gsk.com/dm/1.0/UMLSConcept> .
        {
            ?s <http://gsk-kg.rdip.gsk.com/dm/1.0/conceptLink> ?o .
        } UNION {
            ?s <http://gsk-kg.rdip.gsk.com/dm/1.0/stringLiteral> ?o .
        }
        FILTER (regex(str(?o),"https://meshb.nlm.nih.gov/record/ui")
            || regex(str(?o),"http://amigo.geneontology.org/amigo/term/")
        ).
    }
    """

  // Collect labels for terms connected to lungs,
  // or label for the concrete term if the node connected is a BNode
  // This is mainly a test for FILTER in different positions
  val q19 = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT DISTINCT ?conc ?ps ?s ?p ?o ?label WHERE {
        ?s ?p <http://purl.obolibrary.org/obo/UBERON_0002048> .
        {
            ?s rdfs:label ?label
            FILTER (!isBlank(?s))
        } UNION {
            ?conc ?ps ?s .
            ?conc rdfs:label ?label .
            FILTER isBlank(?s)
        }
        FILTER regex(?label, "lung")
        BIND (<http://purl.obolibrary.org/obo/UBERON_0002048> AS ?o)
    }
    """

  // Construct a literature mapping result
  // Tests CONSTRUCT and string replacement
  val q20 = """
    PREFIX lita: <http://lit-search-api/attribute/>
    PREFIX dm: <http://gsk-kg.rdip.gsk.com/dm/1.0/>
    CONSTRUCT {?s lita:has_alias ?ostr} WHERE {
        ?s a dm:UMLSConcept .
        ?s dm:conceptLink ?o .
        # Removes the 'MESH:' from the data
        BIND(REPLACE(STR(?o),"^[^=]+=","MESH:") as ?ostr) .
    }
    """

  // Simple "What does the KG say about X" query
  // Can be run over literature-extracted triples.
  val q21 = """
    PREFIX  schema: <http://schema.org/>
    PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>
    PREFIX  prism: <http://prismstandard.org/namespaces/basic/2.0/>
    SELECT ?objText ?objStartIdx ?objEndIdx ?subjText ?subjStartIdx ?subjEndIdx ?predText ?doc ?doi ?src ?year ?month ?title ?te ?text
    WHERE
    {
      ?pred dm:hasSubject ?subjde .
      ?pred dm:hasObject ?objde .
      ?objde dm:entityLink ?objle .
      ?subjde dm:entityLink ?subjle .
      # Chose CUI for MCL1
      ?objle dm:mappedTo <http://gsk-kg.rdip.gsk.com/umls/CUI=C1366587> .
      # Chose CUI for HMGCR
      ?subjle dm:mappedTo <http://gsk-kg.rdip.gsk.com/umls/CUI=C1415615> .
      ?subjde dm:indexStart ?subjStartIdx .
      ?subjde dm:indexEnd ?subjEndIdx .
      ?subjde dm:text ?subjText .
      ?objde dm:indexStart ?objStartIdx .
      ?objde dm:indexStart ?objEndIdx .
      ?objde dm:text ?objText .
      ?te dm:contains ?pred .
      ?te dm:text ?text .
      ?ds dm:contains ?te .
      ?doc dm:contains ?ds .
      ?doc schema:title ?title .
      ?doc dm:docSource ?src .
      ?doc prism:doi ?doi .
      ?doc dm:pubDateYear ?year .
      ?doc dm:pubDateMonth ?month .
    }
    """

  // Comprehensive queries

  // Get a sample of triples joining non-blank nodes
  val q22 = "SELECT ?s ?p ?o WHERE {?s ?p ?o . FILTER (!isBlank(?s) && !isBlank(?o))} LIMIT 10"

  // Check DISTINCT works on a small dataset
  val q23 = "SELECT DISTINCT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 10"

  // Get class parent-child relations
  val q24 = """
    SELECT ?s ?o WHERE {
        ?s <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?o .
        FILTER (!isBlank(?s) && !isBlank(?o))
    }
    """

  // Get class parent-child relations with optional labels
  // Tests OPTIONAL
  val q25 = """
    SELECT ?s ?s_name ?o ?o_name WHERE {
        ?s <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?o .
        FILTER (!isBlank(?s) && !isBlank(?o)) .
        OPTIONAL {?s <http://www.w3.org/2000/01/rdf-schema#label> ?s_name}
        OPTIONAL {?o <http://www.w3.org/2000/01/rdf-schema#label> ?o_name}
    }
    """

  // Get all labels in file
  val q26 = """
    SELECT DISTINCT ?label WHERE {
        ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label .
    }
    """

  // Get label of owl:Thing
  // This subject should be present in all ontology files
  val q27 = """
    SELECT ?label WHERE {
        <http://www.w3.org/2002/07/owl#Thing>
        <http://www.w3.org/2000/01/rdf-schema#label>
        ?label .
    }
    """

  // Get label of owl:Thing with prefix
  // This tests prefixes
  val q28 = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    SELECT ?label WHERE {
        owl:Thing <http://www.w3.org/2000/01/rdf-schema#label> ?label .
    }
    """

  // Get label of owl:Thing with explanatory comment
  // This tests comments
  val q29 = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    SELECT ?label WHERE {
        owl:Thing  # owl:Thing is the base class of all possible entities in ontologies
        <http://www.w3.org/2000/01/rdf-schema#label>
        ?label .
    }
    """

  // Get label of owl:Thing with regex to remove poor label if present
  // This tests regexes
  val q30 = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    SELECT ?label WHERE {
        owl:Thing <http://www.w3.org/2000/01/rdf-schema#label> ?label .
        FILTER regex(?label, "^(?!.*(owl:))") .
    }
    """

  // Construct a graph where everything which is a Thing is asserted to exist
  // This tests CONSTRUCT with prefixes and custom IRIs
  val q31 = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    CONSTRUCT {?x <test_schema#ontologyExists> true} WHERE {
        ?x rdfs:subClassOf owl:Thing
    }
    """

  // Construct a graph where all the terms derived from a species have a new relation
  // joining them to their species source.
  // This probably only produces results with CLO.
  val q32 = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX obo-term: <http://purl.obolibrary.org/obo/>
    CONSTRUCT {?cell_line <test_schema#sourceSpecies> ?species} WHERE {
        ?cell_line rdfs:subClassOf ?derived_node .
        ?derived_node a owl:Restriction;
            owl:onProperty obo-term:RO_0001000;
            owl:someValuesFrom ?species_node .
        ?species_node a owl:Restriction;
            owl:onProperty obo-term:BFO_0000050;
            owl:someValuesFrom ?species .
    }
    """

  // Detect punned relations in an ontology
  // This is when a node is used both as a class and a property
  // It breaks some OWL parsers and these terms should be cleaned up or
  // removed if possible
  val q33 = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT ?s ?o
    WHERE {
         ?s a ?o .
         ?o rdfs:subClassOf rdf:Property .
         ?s a owl:Class .
    }
    """

  // Construct a triple where the predicate is derived
  // from the object of a triple in the WHERE clause.
  // This works on the semantic network file only.
  // Also tests UNION.
  val q34 = """
    PREFIX  litp:  <http://lit-search-api/property/>
    PREFIX  sn: <http://gsk-kg.rdip.gsk.com/semanticnetwork/>
    CONSTRUCT
    {
        ?c1 ?rel ?c2 .
        ?rel litp:name ?relstr .
    }
    WHERE
    {
        {
            ?lnk sn:STY_RL1 ?c1 .
            ?lnk sn:RL ?rel .
            ?rel sn:STY_RL ?relstr .
            ?lnk sn:STY_RL2 ?c2 .
        }
        UNION
        {
            ?lnk sn:STY1 ?c1 .
            ?lnk sn:RL ?rel .
            ?rel sn:STY_RL ?relstr .
            ?lnk sn:STY2 ?c2 .
        }
    }
    """

  // Query to convert schema of predications.
  // Can be run on literature-extracted triples.
  // Tests CONSTRUCT, BIND, join over blank nodes,
  // and OPTIONAL.
  val q35 = """
    PREFIX  dm:   <http://gsk-kg.rdip.gsk.com/dm/1.0/>
    PREFIX  litp:  <http://lit-search-api/property/>
    PREFIX  lita:  <http://lit-search-api/attribute/>
    PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
    CONSTRUCT
    {
        ?docsec litp:containsPredication ?pred .
        ?pred litp:hasSubj ?detentSubj .
        ?pred litp:hasObj ?detentObj .
        ?pred lita:docId ?docid .
        ?pred lita:relationClass ?cls .
        ?pred lita:confidence ?conf .
        ?conf a xsd:float .
    }
    WHERE
    {
        ?d a dm:Document .
        ?d dm:contains ?ds .
        ?ds a dm:DocSection .
        ?ds dm:contains ?te .
        ?te a dm:TextElement .
        ?te dm:contains ?p .
        ?p a dm:Predication .
        ?p dm:hasSubject ?deSubj .
        ?p dm:hasObject ?deObj .
        ?p dm:predRelation ?b .
        ?b dm:predClass ?cls .
        OPTIONAL { ?b dm:confidence ?conf }
        BIND(STRAFTER(str(?d), "#") as ?docid) .
        BIND(STRAFTER(str(?ds), "#") as ?secid) .
        BIND(URI(CONCAT("http://lit-search-api/node/docsec#", ?secid)) as ?docsec) .
        BIND(STRAFTER(str(?deSubj), "#") as ?entidSubj) .
        BIND(URI(CONCAT("http://lit-search-api/node/entity#", ?entidSubj)) as ?detentSubj) .
        BIND(STRAFTER(str(?deObj), "#") as ?entidObj) .
        BIND(URI(CONCAT("http://lit-search-api/node/entity#", ?entidObj)) as ?detentObj) .
        BIND(STRAFTER(str(?p), "#") as ?predid) .
        BIND(URI(CONCAT("http://lit-search-api/node/predication#", ?predid)) as ?pred)
    }
    """

  // Query that combines FROM and FROM NAMED graph to default graph and
  // FROM NAMED as named graphs in the same query. It uses GRAPH expression
  // with a specific URI so it will match that specific graph
  val q36 =
  """
    |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    |PREFIX dc: <http://purl.org/dc/elements/1.1/>
    |PREFIX ex: <http://example.org/>
    |
    |SELECT ?who ?mbox
    |FROM <http://example.org/dft.ttl>
    |FROM NAMED <http://example.org/alice>
    |FROM NAMED <http://example.org/bob>
    |WHERE
    |{
    |   GRAPH ex:alice { ?x foaf:mbox ?mbox }
    |}
    |""".stripMargin

  // Query that combines FROM for adding graph to default graph and
  // FROM NAMED as named graphs in the same query. It uses GRAPH expression
  // with a variable so it can match multiple graphs.
  val q37 =
    """
      |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      |PREFIX dc: <http://purl.org/dc/elements/1.1/>
      |PREFIX ex: <http://example.org/>
      |
      |SELECT ?who ?g ?mbox
      |FROM <http://example.org/dft.ttl>
      |FROM NAMED <http://example.org/alice>
      |FROM NAMED <http://example.org/bob>
      |WHERE
      |{
      |   ?g dc:publisher ?who .
      |   GRAPH ?g { ?x foaf:mbox ?mbox }
      |}
      |""".stripMargin
}
