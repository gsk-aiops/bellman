import xerial.sbt.Sonatype._

lazy val Versions = Map(
  "kind-projector"     -> "0.11.3",
  "cats"               -> "2.0.0",
  "jena"               -> "3.17.0",
  "scalatest"          -> "3.2.5",
  "fastparse"          -> "2.1.2",
  "cats"               -> "2.0.0",
  "scala212"           -> "2.12.12",
  "scala211"           -> "2.11.12",
  "droste"             -> "0.8.0",
  "spark"              -> "2.4.0",
  "spark-testing-base" -> "2.4.5_0.14.0",
  "jackson"            -> "2.12.1",
  "scalacheck"         -> "1.15.2",
  "scalatestplus"      -> "3.2.3.0",
  "sansa"              -> "0.7.1",
)

inThisBuild(List(
  organization := "com.github.gsk-aiops",
  homepage := Some(url("https://github.com/gsk-aiops/bellman-algebra-parser")),
  licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  developers := List(
    Developer(
      id = "JNKHunter",
      name = "John Hunter",
      email = "johnhuntergskatgmail.com",
      url = url("https://gsk.com")
    )
  )
))

lazy val buildSettings = Seq(
  scalaVersion := Versions("scala211"),
  crossScalaVersions :=  List(Versions("scala211"), Versions("scala212")),
  sonatypeProjectHosting := Some(GitHubHosting("gsk-aiops", "bellman-algebra-parser", "johnhuntergskatgmail.com")),
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
  skip in publish := true
)

lazy val compilerPlugins = Seq(
  libraryDependencies ++= Seq(
    compilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full),
    compilerPlugin("org.typelevel" %% "kind-projector" % Versions("kind-projector") cross CrossVersion.full),
  )
)

lazy val commonDependencies = Seq(
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-core" % Versions("cats"),
    "io.higherkindness" %% "droste-core" % Versions("droste"),
    "io.higherkindness" %% "droste-macros" % Versions("droste"),
    "org.scalatest" %% "scalatest" % Versions("scalatest") % Test,
  )
)

lazy val bellman = project
  .in(file("."))
  .settings(buildSettings)
  .settings(noPublishSettings)
  .dependsOn(`bellman-algebra-parser`, `bellman-spark-engine`)
  .aggregate(`bellman-algebra-parser`, `bellman-spark-engine`)

lazy val `bellman-algebra-parser` = project
  .in(file("modules/parser"))
  .settings(moduleName := "bellman-algebra-parser")
  .settings(buildSettings)
  .settings(commonDependencies)
  .settings(compilerPlugins)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.jena" % "jena-arq" % Versions("jena"),
      "com.lihaoyi" %% "fastparse" % Versions("fastparse"),
    ),
  )

lazy val `bellman-spark-engine` = project
  .in(file("modules/engine"))
  .settings(moduleName := "bellman-spark-engine")
  .settings(buildSettings)
  .settings(commonDependencies)
  .settings(compilerPlugins)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-sql"          % Versions("spark") % Provided,
      "com.holdenkarau"   %% "spark-testing-base" % Versions("spark-testing-base") % Test,
      "org.scalacheck"    %% "scalacheck"         % Versions("scalacheck") % Test,
      "org.scalatestplus" %% "scalacheck-1-15"    % Versions("scalatestplus") % Test,
    ),
    libraryDependencies ++= on(2, 11)(
      "net.sansa-stack" %% "sansa-rdf-spark" % Versions("sansa") % Test
    ).value,
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % Versions("jackson"),
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions("jackson"),
    )
  )
  .dependsOn(`bellman-algebra-parser`)

addCommandAlias("ci-test", ";scalastyle ;+test")

def on[A](major: Int, minor: Int)(a: A): Def.Initialize[Seq[A]] =
  Def.setting {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some(v) if v == (major, minor) => Seq(a)
      case _                              => Nil
    }
  }
