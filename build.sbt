import xerial.sbt.Sonatype._

lazy val Versions = Map(
  "kind-projector"       -> "0.11.3",
  "cats"                 -> "2.0.0",
  "cats-scalacheck"      -> "0.2.0",
  "jena"                 -> "3.17.0",
  "scalatest"            -> "3.2.8",
  "fastparse"            -> "2.1.2",
  "cats"                 -> "2.0.0",
  "scala212"             -> "2.12.12",
  "scala211"             -> "2.11.12",
  "droste"               -> "0.8.0",
  "spark"                -> "2.4.0",
  "spark-testing-base"   -> "2.4.5_0.14.0",
  "jackson"              -> "2.12.1",
  "scalacheck"           -> "1.15.2",
  "scalatestplus"        -> "3.2.3.0",
  "sansa"                -> "0.7.1",
  "monocle"              -> "1.5.0",
  "discipline"           -> "1.1.2",
  "discipline-scalatest" -> "2.1.4",
  "reftree"              -> "1.4.0",
  "shims"                -> "2.1.0",
  "pureconfig"           -> "0.14.0"
)

inThisBuild(
  List(
    organization := "com.github.gsk-aiops",
    homepage := Some(
      url("https://github.com/gsk-aiops/bellman-algebra-parser")
    ),
    licenses := Seq(
      "APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")
    ),
    developers := List(
      Developer(
        id = "JNKHunter",
        name = "John Hunter",
        email = "johnhuntergskatgmail.com",
        url = url("https://gsk.com")
      )
    ),
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    scalacOptions += "-Ywarn-unused-import",
    scalafixDependencies := Seq(
      "com.github.liancheng" %% "organize-imports" % "0.5.0",
      "com.github.vovapolu"  %% "scaluzzi"         % "0.1.18"
    )
  )
)

lazy val buildSettings = Seq(
  scalaVersion := Versions("scala211"),
  crossScalaVersions := List(Versions("scala211"), Versions("scala212")),
  sonatypeProjectHosting := Some(
    GitHubHosting(
      "gsk-aiops",
      "bellman-algebra-parser",
      "johnhuntergskatgmail.com"
    )
  ),
  sonatypeProfileName := "com.github.gsk-aiops",
  artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
    artifact.name + "_" + sv.binary + "-" + module.revision + "." + artifact.extension
  },
  scalastyleFailOnWarning := true
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false,
  publish / skip := true
)

lazy val compilerPlugins = Seq(
  libraryDependencies ++= Seq(
    compilerPlugin(
      "org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full
    ),
    compilerPlugin(
      "org.typelevel" %% "kind-projector" % Versions(
        "kind-projector"
      ) cross CrossVersion.full
    )
  )
)

lazy val commonDependencies = Seq(
  libraryDependencies ++= Seq(
    "org.typelevel"         %% "cats-core"     % Versions("cats"),
    "io.higherkindness"     %% "droste-core"   % Versions("droste"),
    "io.higherkindness"     %% "droste-macros" % Versions("droste"),
    "com.github.pureconfig" %% "pureconfig"    % Versions("pureconfig"),
    "org.scalatest"         %% "scalatest"     % Versions("scalatest") % Test
  )
)

lazy val bellman = project
  .in(file("."))
  .settings(buildSettings)
  .settings(noPublishSettings)
  .dependsOn(`bellman-algebra-parser`, `bellman-spark-engine`)
  .aggregate(`bellman-algebra-parser`, `bellman-spark-engine`, `bellman-site`)

lazy val `bellman-algebra-parser` = project
  .in(file("modules/parser"))
  .settings(moduleName := "bellman-algebra-parser")
  .settings(buildSettings)
  .settings(commonDependencies)
  .settings(compilerPlugins)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.jena" % "jena-arq"  % Versions("jena"),
      "com.lihaoyi"    %% "fastparse" % Versions("fastparse")
    )
  )

lazy val `bellman-spark-engine` = project
  .in(file("modules/engine"))
  .settings(moduleName := "bellman-spark-engine")
  .settings(buildSettings)
  .settings(commonDependencies)
  .settings(compilerPlugins)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark"           %% "spark-sql"    % Versions("spark") % Provided,
      "com.github.julien-truffaut" %% "monocle-core" % Versions("monocle"),
      "com.github.julien-truffaut" %% "monocle-macro" % Versions("monocle"),
      "com.github.julien-truffaut" %% "monocle-law" % Versions(
        "monocle"
      )                 % Test,
      "com.codecommit" %% "shims" % Versions("shims") % Test,
      "org.typelevel"  %% "discipline-core" % Versions("discipline") % Test,
      "org.typelevel"  %% "discipline-scalatest" % Versions(
        "discipline-scalatest"
      )                    % Test,
      "io.chrisdavenport" %% "cats-scalacheck" % Versions(
        "cats-scalacheck"
      )                  % Test,
      "com.holdenkarau" %% "spark-testing-base" % Versions(
        "spark-testing-base"
      )                    % Test,
      "org.scalacheck"    %% "scalacheck" % Versions("scalacheck") % Test,
      "org.scalatestplus" %% "scalacheck-1-15" % Versions(
        "scalatestplus"
      ) % Test
    ),
    libraryDependencies ++= on(2, 11)(
      "net.sansa-stack" %% "sansa-rdf-spark" % Versions("sansa") % Test
    ).value,
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core"    % "jackson-databind" % Versions("jackson"),
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions(
        "jackson"
      )
    )
  )
  .settings(
    Compile / console / scalacOptions ~= {
      _.filterNot(Set("-Ywarn-unused-import", "-Ywarn-unused:imports"))
    },
    console / initialCommands := """
    import cats._
    import cats.implicits._

    import higherkindness.droste._
    import higherkindness.droste.data._
    import higherkindness.droste.data.prelude._
    import higherkindness.droste.syntax.all._

    import com.gsk.kg.Graphs
    import com.gsk.kg.config.Config
    import com.gsk.kg.sparql.syntax.all._
    import com.gsk.kg.sparqlparser._
    import com.gsk.kg.engine.data._
    import com.gsk.kg.engine.data.ToTree._
    import com.gsk.kg.engine._
    import com.gsk.kg.engine.DAG._
    import com.gsk.kg.engine.optimizer._
    import com.gsk.kg.engine.syntax._

    import org.apache.spark._
    import org.apache.spark.sql._
    
    import pureconfig.generic.auto._
    import pureconfig._

    val spark = SparkSession.builder()
      .appName("Spark Local")
      .master("local")
      .config("spark.driver.host", "localhost")
      .getOrCreate()
      
    implicit val sc: SQLContext = spark.sqlContext
    val config = ConfigSource.default.loadOrThrow[Config]

    import sc.implicits._

    def readNTtoDF(path: String) = {
      import org.apache.jena.riot.RDFParser
      import org.apache.jena.riot.lang.CollectorStreamTriples
      import scala.collection.JavaConverters._

      val filename                            = s"modules/engine/src/test/resources/$path"
      val inputStream: CollectorStreamTriples = new CollectorStreamTriples()
      RDFParser.source(filename).parse(inputStream)

      inputStream
        .getCollected()
        .asScala
        .toList
        .map(triple =>
          (
            triple.getSubject().toString(),
            triple.getPredicate().toString(),
            triple.getObject().toString(),
            ""
          )
        )
        .toDF("s", "p", "o", "g")
    }

    def printTree(query: String): Unit = {
      val q = QueryConstruct.parse(query, Config.default)._1
      val dag = DAG.fromQuery.apply(q)
      println(dag.toTree.drawTree)
    }
    
    def printOptimizedTree(query: String): Unit = {
      val q = QueryConstruct.parse(query, Config.default)._1
      val dag = Optimizer.optimize.apply((DAG.fromQuery.apply(q), Graphs.empty)).runA(Config.default, null).right.get
      println(dag.toTree.drawTree)
    }
    """
  )
  .settings(
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case _                                   => MergeStrategy.last
    }
  )
  .dependsOn(`bellman-algebra-parser` % "compile->compile;test->test")

lazy val `bellman-site` = project
  .in(file("modules/site"))
  .settings(moduleName := "bellman-site")
  .settings(buildSettings)
  .settings(noPublishSettings)
  .settings(commonDependencies)
  .settings(compilerPlugins)
  .settings(
    micrositeName := "Bellman",
    micrositeDescription := "Efficiently running SparQL queries in Spark",
    micrositeGithubOwner := "gsk-aiops",
    micrositeGithubRepo := "bellman",
    micrositeOrganizationHomepage := "https://www.gsk.com",
    micrositeBaseUrl := "bellman",
    micrositeDocumentationUrl := "docs/compilation",
    micrositeGitterChannel := false,
    micrositePushSiteWith := GitHub4s,
    mdocIn := (Compile / sourceDirectory).value / "docs",
    micrositeGithubToken := Option(System.getenv().get("GITHUB_TOKEN")),
    micrositeImgDirectory := (Compile / resourceDirectory).value / "site" / "images" / "overview",
    micrositeHighlightTheme := "tomorrow",
    micrositeTheme := "light",
    micrositePalette := Map(
      "brand-primary"   -> "#868b8e",
      "brand-secondary" -> "#b9b7bd",
      "white-color"     -> "#fff"
    ),
    micrositeHighlightTheme := "github-gist"
  )
  .settings(
    libraryDependencies += "io.github.stanch" %% "reftree" % Versions("reftree")
  )
  .settings(
    Global / excludeLintKeys += scalastyleFailOnWarning
  )
  .dependsOn(`bellman-spark-engine`, `bellman-algebra-parser`)
  .disablePlugins(ScalastylePlugin)
  .enablePlugins(MicrositesPlugin)

addCommandAlias(
  "build-microsite",
  ";bellman-site/run ;bellman-site/makeMicrosite"
)

addCommandAlias(
  "ci-test",
  ";scalafixAll --check ;scalafmtCheckAll ;scalastyle ;+test ;build-microsite"
)

addCommandAlias(
  "ci-docs",
  ";build-microsite ;bellman-site/publishMicrosite"
)

addCommandAlias(
  "lint",
  ";scalafixAll ;scalafmtAll ;scalastyle"
)

def on[A](major: Int, minor: Int)(a: A): Def.Initialize[Seq[A]] =
  Def.setting {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some(v) if v == (major, minor) => Seq(a)
      case _                              => Nil
    }
  }
