// When the user clicks on the search box, we want to toggle the search dropdown
function displayToggleSearch(e) {
  e.preventDefault();
  e.stopPropagation();

  closeDropdownSearch(e);
  
  if (idx === null) {
    console.log("Building search index...");
    prepareIdxAndDocMap();
    console.log("Search index built.");
  }
  const dropdown = document.querySelector("#search-dropdown-content");
  if (dropdown) {
    if (!dropdown.classList.contains("show")) {
      dropdown.classList.add("show");
    }
    document.addEventListener("click", closeDropdownSearch);
    document.addEventListener("keydown", searchOnKeyDown);
    document.addEventListener("keyup", searchOnKeyUp);
  }
}

//We want to prepare the index only after clicking the search bar
var idx = null
const docMap = new Map()

function prepareIdxAndDocMap() {
  const docs = [  
    {
      "title": "compactBgps",
      "url": "/docs/optimization/compactBgps",
      "content": "BGP Compaction Basic Graph Patterns are the sets of triples that we write on queries to express the edges of the graph we want to focus on, for example: ?d a &lt;http://example.com/Document&gt; . ?d &lt;http://example.com/HasSource&gt; &lt;http://example.com/MySource&gt; In this case these two triples express we want to get nodes that have the type doc:Document, and that have a specific source. In order to fulfill this query, we would need to perform two queries to the underlying dataset, and then join the results by the shared variables (?d in this case). What we’ll do to perform BGP compaction is to store the triples in the BGP into a special purpose data structure called ChunkedList, that has a compact operation. Introducing ChunkedList ChunkedList is a special purpose data structure that looks like a linked list from the outside, with a head and a tail. The trick we do is that we put elements not directly in the head, but inside other linear data structure from Cats, called NonEmptyChain. We use NonEmptyChain as our Chunk type because it has efficient prepends and appends, and we need that in the implementation. It looks like this: sealed trait ChunkedList[A] object ChunkedList { type Chunk[A] = NonEmptyChain[A] val Chunk = NonEmptyChain final case class Empty[A]() extends ChunkedList[A] final case class NonEmpty[A](head: Chunk[A], tail: ChunkedList[A]) extends ChunkedList[A] } The difference that our ChunkedList has from other data structures comes from the compact method. compact will try to merge elements into the same chunk given an auxiliary function. The signature looks like this: def compact[B](f: A =&gt; B)(implicit B: Eq[B]): ChunkedList[A] = ??? compact has some nice properties too. An already compacted list cannot be further compacted, for example, we can express that with a law like this: val list: ChunkedList[Int] = ??? list.compact(fn).compact(fn) === list.compact(fn) Here we can see how ChunkedList compaction looks. In this example we’re compacting it by the first letter of each element, making it group into two chunks, those elements starting with “a”, and those starting with “b”. val list: ChunkedList[String] = ChunkedList.fromList(List(\"a\", \"as\", \"asterisk\", \"b\", \"bee\", \"burrito\")) list.compact(_.head) Compacting BGPs by shared variables The auxiliary function we have for compacting BGPs looks for shared variables in the same positions in the triples. Now that we have the triples compacted in the BGP, what we do is that we query the underlying datafram once per chunk, not once per triple. You can take a look at the Engine file to see how we iterate over all chunks to query the dataframe."
    } ,    
    {
      "title": "Compilation",
      "url": "/docs/compilation",
      "content": "Compilation The process of querying the underlying Spark datasest is dictated by SparQL algebra. Bellman expects a three (or four) column dataset representing a graph, in which the first column represents the subject,the second one the predicate, and the third one the object. It’s optional to add a fourth column representing the graph to which the edge belongs. What we do in Bellman is, for each triple in the Basic Graph Pattern, query the dataframe, and then join them given their common variables, for example: SELECT ?d ?author WHERE { ?d rdf:type doc:Document . ?d doc:Author ?author } This query selects documents with their author. In Bellman we query once per triple, so starting with the first triple: ?d rdf:type doc:Document We will query the dataframe like this: val triple1Result = df.select($\"p\" === \"rdf:type\" &amp;&amp;&amp; $\"o\" === \"doc:Document\") And for the second triple we will do: val triple2Result = df.select($\"$p\" === \"doc:Author\") Finally, with these two values, we will join them on the common variables, in this case the ?d column only: val result = triple1Result.join(triple2Result, \"s\")"
    } ,    
    {
      "title": "Engine",
      "url": "/docs/phases/engine",
      "content": "Engine The Engine receives the optimized graph, and transforms it to Spark jobs, as explained in the Compilation page."
    } ,    
    {
      "title": "Graph Pushdown",
      "url": "/docs/optimization/graphPushdown",
      "content": "Graph pushdown There’s a feature in SparQL that allows users to query only a specific graph, not the whole graph. One can perform that using named graphs and the GRAPH statement, here’s an example: SELECT * FROM NAMED &lt;http://example.com/named-graph&gt; { GRAPH &lt;http://example.com/named-graph&gt; { ?s &lt;http://example.com/predicate&gt; ?o } } By default GRAPH is captured as a node in the AST but what we do is treat the graph as another component of triples, making them Quads. In our Graph Pushdown optimization we send graph information to the quad level, removing the GRAPH node from the AST. We use that information as any other element from the triple when querying the underlying DataFrame."
    } ,        
    {
      "title": "Optimizer",
      "url": "/docs/phases/optimizer",
      "content": "Optimization We run optimization in order to make the Spark job run faster, and avoid needless nodes in the DAG."
    } ,    
    {
      "title": "Optimization",
      "url": "/docs/optimization",
      "content": "Optimization There are some optimizations that Bellman performs to make queries run faster. Graph pushdown Compact BGPs Remove Nested Project"
    } ,    
    {
      "title": "Parser",
      "url": "/docs/phases/parser",
      "content": "Parser Parser transforms the String representing the query into the Expr AST. This Expr AST is the representation of the SparQL algebra. The process of converting the string to our AST is done using Apache Jena. We parse intro Jena Algebra, transform that to the lisp like string that Jena uses for the algebra, and then parse that to Scala datatypes."
    } ,    
    {
      "title": "Phases",
      "url": "/docs/phases",
      "content": "Phases Bellman is architected as a nanopass compiler, using Recursion Schemes as a framework. The main phases are the following: parser transforms query strings to a Query ADT transformToGraph creates the DAG we handle internally in Bellman optimizer runs optimizations on the DAG staticAnalysis performs some analysis on the query to reject bad queries engine actually runs the query in Spark rdfFormatter converts the result to a DataFrame with RDF formatted values"
    } ,    
    {
      "title": "RDF Formatter",
      "url": "/docs/phases/rdfFormatter",
      "content": "RDF Formatter The RDF Formatter phase runs after the results have been received back from Spark, and transforms the dataset to adapt it to the constraints that RDF has on values. It does some conversions, such as transforming numbers from their int representation 1 to their RDF one \"1\"^^xsd:int."
    } ,      
    {
      "title": "Static Analysis",
      "url": "/docs/phases/staticAnalysis",
      "content": "Static Analysis We perform some static analysis on the query in order to reject invalid queries before sending them to the Spark cluster. Currently we check that the variables used in the query are bound, but more analysis may come in the future."
    } ,    
    {
      "title": "Techniques",
      "url": "/docs/techniques",
      "content": "Techniques recursion schemes folding over a carrier function"
    } ,    
    {
      "title": "Graph Transformation",
      "url": "/docs/phases/transformToGraph",
      "content": "Graph transformation The datatype that the parser returns, Expr is not very well suited to running into Spark DataFrames. In order to simplify compilation into Spark DataFrames, we transform values from Expr datatype into our internal DAG. The internal DAG has several advantages: It contains queries as cases of the ADT, not as an external Query object. Avoids duplication of some SparQL algebrae."
    }    
  ];

  idx = lunr(function () {
    this.ref("title");
    this.field("content");

    docs.forEach(function (doc) {
      this.add(doc);
    }, this);
  });

  docs.forEach(function (doc) {
    docMap.set(doc.title, doc.url);
  });
}

// The onkeypress handler for search functionality
function searchOnKeyDown(e) {
  const keyCode = e.keyCode;
  const parent = e.target.parentElement;
  const isSearchBar = e.target.id === "search-bar";
  const isSearchResult = parent ? parent.id.startsWith("result-") : false;
  const isSearchBarOrResult = isSearchBar || isSearchResult;

  if (keyCode === 40 && isSearchBarOrResult) {
    // On 'down', try to navigate down the search results
    e.preventDefault();
    e.stopPropagation();
    selectDown(e);
  } else if (keyCode === 38 && isSearchBarOrResult) {
    // On 'up', try to navigate up the search results
    e.preventDefault();
    e.stopPropagation();
    selectUp(e);
  } else if (keyCode === 27 && isSearchBarOrResult) {
    // On 'ESC', close the search dropdown
    e.preventDefault();
    e.stopPropagation();
    closeDropdownSearch(e);
  }
}

// Search is only done on key-up so that the search terms are properly propagated
function searchOnKeyUp(e) {
  // Filter out up, down, esc keys
  const keyCode = e.keyCode;
  const cannotBe = [40, 38, 27];
  const isSearchBar = e.target.id === "search-bar";
  const keyIsNotWrong = !cannotBe.includes(keyCode);
  if (isSearchBar && keyIsNotWrong) {
    // Try to run a search
    runSearch(e);
  }
}

// Move the cursor up the search list
function selectUp(e) {
  if (e.target.parentElement.id.startsWith("result-")) {
    const index = parseInt(e.target.parentElement.id.substring(7));
    if (!isNaN(index) && (index > 0)) {
      const nextIndexStr = "result-" + (index - 1);
      const querySel = "li[id$='" + nextIndexStr + "'";
      const nextResult = document.querySelector(querySel);
      if (nextResult) {
        nextResult.firstChild.focus();
      }
    }
  }
}

// Move the cursor down the search list
function selectDown(e) {
  if (e.target.id === "search-bar") {
    const firstResult = document.querySelector("li[id$='result-0']");
    if (firstResult) {
      firstResult.firstChild.focus();
    }
  } else if (e.target.parentElement.id.startsWith("result-")) {
    const index = parseInt(e.target.parentElement.id.substring(7));
    if (!isNaN(index)) {
      const nextIndexStr = "result-" + (index + 1);
      const querySel = "li[id$='" + nextIndexStr + "'";
      const nextResult = document.querySelector(querySel);
      if (nextResult) {
        nextResult.firstChild.focus();
      }
    }
  }
}

// Search for whatever the user has typed so far
function runSearch(e) {
  if (e.target.value === "") {
    // On empty string, remove all search results
    // Otherwise this may show all results as everything is a "match"
    applySearchResults([]);
  } else {
    const tokens = e.target.value.split(" ");
    const moddedTokens = tokens.map(function (token) {
      // "*" + token + "*"
      return token;
    })
    const searchTerm = moddedTokens.join(" ");
    const searchResults = idx.search(searchTerm);
    const mapResults = searchResults.map(function (result) {
      const resultUrl = docMap.get(result.ref);
      return { name: result.ref, url: resultUrl };
    })

    applySearchResults(mapResults);
  }

}

// After a search, modify the search dropdown to contain the search results
function applySearchResults(results) {
  const dropdown = document.querySelector("div[id$='search-dropdown'] > .dropdown-content.show");
  if (dropdown) {
    //Remove each child
    while (dropdown.firstChild) {
      dropdown.removeChild(dropdown.firstChild);
    }

    //Add each result as an element in the list
    results.forEach(function (result, i) {
      const elem = document.createElement("li");
      elem.setAttribute("class", "dropdown-item");
      elem.setAttribute("id", "result-" + i);

      const elemLink = document.createElement("a");
      elemLink.setAttribute("title", result.name);
      elemLink.setAttribute("href", result.url);
      elemLink.setAttribute("class", "dropdown-item-link");

      const elemLinkText = document.createElement("span");
      elemLinkText.setAttribute("class", "dropdown-item-link-text");
      elemLinkText.innerHTML = result.name;

      elemLink.appendChild(elemLinkText);
      elem.appendChild(elemLink);
      dropdown.appendChild(elem);
    });
  }
}

// Close the dropdown if the user clicks (only) outside of it
function closeDropdownSearch(e) {
  // Check if where we're clicking is the search dropdown
  if (e.target.id !== "search-bar") {
    const dropdown = document.querySelector("div[id$='search-dropdown'] > .dropdown-content.show");
    if (dropdown) {
      dropdown.classList.remove("show");
      document.documentElement.removeEventListener("click", closeDropdownSearch);
    }
  }
}
