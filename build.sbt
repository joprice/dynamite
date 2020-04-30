import sbtrelease.ReleaseStateTransformations._
import ohnosequences.sbt.GithubRelease
import org.kohsuke.github.GHRelease

enablePlugins(JavaAppPackaging, BuildInfoPlugin)

buildInfoPackage := "dynamite"

scalaVersion := "2.13.2"

mainClass in Compile := Some("dynamite.Dynamite")

fork := true

addCommandAlias(
  "validate",
  Seq(
    "clean",
    //"coverage",
    "test",
    //"coverageReport"
    "scalafmtCheck"
  ).mkString(";", ";", "")
)

addCommandAlias("dynamodbLocal", "dynamite/test:runMain dynamite.DynamoDBLocal")

val dynamodbLocalProperties = Seq(
  "-Dsqlite4java.library.path=native-libs"
)

javaOptions in Test ++= dynamodbLocalProperties

lazy val copyJars = taskKey[Unit]("copyJars")

val dynamodbLocalVersion = "1.0.392"

copyJars := {
  // See http://softwarebyjosh.com/2018/03/25/how-to-unit-test-your-dynamodb-queries.html
  import java.nio.file.Files
  import java.io.File
  val artifactTypes = Set("dylib", "so", "dll")
  val files = Classpaths.managedJars(Test, artifactTypes, update.value).files
  val nativeLibs = new File(baseDirectory.value, "native-libs")
  Files.createDirectories(nativeLibs.toPath)
  files.foreach { f =>
    // NOTE: this version must be trimmed, otherwise it will not be found
    // by sqlite
    val fileToCopy = new File(nativeLibs, f.name.replace(s"-$dynamodbLocalVersion", ""))
    if (!fileToCopy.exists()) {
      println(s"Copying $f to $fileToCopy")
      Files.copy(f.toPath, fileToCopy.toPath)
    }
  }
}

bashScriptExtraDefines += """addJava "-Dsqlite4java.library.path=$(realpath ${app_home}/../native-libs)""""

mappings in Universal ++= NativePackagerHelper.directory("native-libs").map { t =>
  (t._1, t._2)
}

(compile in Compile) := (compile in Compile).dependsOn(copyJars).value

resolvers ++= Seq(
  "dynamodb-local-oregon" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"
)

// for DynamoDBLocal
classpathTypes ++= Set("dylib", "so")

val zioVersion = "1.0.0-RC18-2"
val zioConfigVersion = "1.0.0-RC16-2"
val awsVersion = "1.11.770"

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "fastparse" % "2.3.0",
  "com.amazonaws" % "aws-java-sdk-dynamodb" % awsVersion,
  "com.amazonaws" % "aws-java-sdk-sts" % awsVersion,
  "jline" % "jline" % "2.14.6",
  "com.lihaoyi" %% "fansi" % "0.2.9",
  "com.github.scopt" %% "scopt" % "3.7.1",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "com.typesafe" % "config" % "1.4.0",
  "dev.zio" %% "zio-config" % zioConfigVersion,
  "dev.zio" %% "zio-config-typesafe" % zioConfigVersion,
  "dev.zio" %% "zio-config-magnolia" % zioConfigVersion,
  "dev.zio" %% "zio-logging" % "0.2.7",
  "dev.zio" %% "zio-streams" % zioVersion,
  "dev.zio" %% "zio-test" % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
  "com.typesafe.play" %% "play-json" % "2.8.1",
  "com.amazonaws" % "DynamoDBLocal" % "1.11.477",
  "com.almworks.sqlite4java" % "sqlite4java" % dynamodbLocalVersion,
  "com.almworks.sqlite4java" % "libsqlite4java-osx" % dynamodbLocalVersion artifacts (Artifact(
    "libsqlite4java-osx",
    "dylib",
    "dylib"
  )),
  "com.almworks.sqlite4java" % "libsqlite4java-linux-amd64" % dynamodbLocalVersion artifacts (Artifact(
    "libsqlite4java-linux-amd64",
    "so",
    "so"
  ))
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

ghreleaseRepoOrg := "joprice"
ghreleaseRepoName := "dynamite"

//GithubRelease.draft := true

ghreleaseAssets := Seq((packageBin in Universal).value)

scalacOptions in (Compile, compile) ++= Seq(
  "-encoding",
  "UTF-8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-explaintypes",
  "-Yrangepos",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused:patvars,-implicits",
  "-Ywarn-value-discard",
  "-Ybackend-parallelism", math.min(java.lang.Runtime.getRuntime.availableProcessors, 16).toString,
  "-Ycache-plugin-class-loader:always",
  "-P:splain:keepmodules:1"
)

scalacOptions in (Test, compile) ++= (scalacOptions in (Compile, compile)).value

lazy val checkVersionNotes = taskKey[Unit](
  "Checks that the notes for the next version are present to avoid build failures."
)

checkVersionNotes := {
  val notesFile = baseDirectory.value / "notes" / (version.value.stripSuffix(
    "-SNAPSHOT"
  ) + ".markdown")
  val notesPath = notesFile.relativeTo(baseDirectory.value).getOrElse(notesFile)
  if (!notesPath.exists) {
    sys.error(s"Missing notes file $notesPath")
  }
}

lazy val releaseOnGithub = taskKey[GHRelease]("Releases project on github")

releaseOnGithub := Def.taskDyn {
  GithubRelease.defs.githubRelease(s"v${version.value}")
}.value

releaseProcess := Seq[ReleaseStep](
  releaseStepTask(checkVersionNotes),
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  // the new tag needs to be present on github before releasing
  pushChanges,
  releaseStepTask(releaseOnGithub),
  setNextVersion,
  commitNextVersion,
  pushChanges
)

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
addCompilerPlugin("io.tryp" % "splain" % "0.5.4" cross CrossVersion.patch)

// allows using unsafeRun for experimenting in console
consoleQuick / initialCommands := """
import zio._, zio.console._, zio.Runtime.default._
"""

console / initialCommands := (consoleQuick / initialCommands).value + """
import dynamite._
"""

reporterConfig := reporterConfig.value.withShowLegend(false)
reporterConfig := reporterConfig.value.withReverseOrder(true)
