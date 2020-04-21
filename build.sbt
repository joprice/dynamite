import sbtrelease.ReleaseStateTransformations._
import ohnosequences.sbt.GithubRelease
import org.kohsuke.github.GHRelease

enablePlugins(JavaAppPackaging, BuildInfoPlugin)

buildInfoPackage := "dynamite"

scalaVersion := "2.13.1"

mainClass in Compile := Some("dynamite.Dynamite")

addCommandAlias(
  "validate",
  Seq(
    "clean",
    //"coverage",
    "test"
    //"coverageReport"
  ).mkString(";", ";", "")
)

val awsVersion = "1.11.764"

fork := true

lazy val copyJars = taskKey[Unit]("copyJars")

javaOptions in Test ++= Seq(
  "-Dsqlite4java.library.path=native-libs"
)

copyJars := {
  // See http://softwarebyjosh.com/2018/03/25/how-to-unit-test-your-dynamodb-queries.html
  import java.nio.file.Files
  import java.io.File
  val artifactTypes = Set("dylib", "so", "dll")
  val files = Classpaths.managedJars(Test, artifactTypes, update.value).files
  val nativeLibs = new File(baseDirectory.value, "native-libs")
  Files.createDirectories(nativeLibs.toPath)
  files.foreach { f =>
    val fileToCopy = new File(nativeLibs, f.name)
    if (!fileToCopy.exists()) {
      println(s"Copying $f to $fileToCopy")
      Files.copy(f.toPath, fileToCopy.toPath)
    }
  }
}

(compile in Compile) := (compile in Compile).dependsOn(copyJars).value

resolvers ++= Seq(
  "dynamodb-local-oregon" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"
)

// for DynamoDBLocal
classpathTypes ++= Set("dylib", "so")

val zioVersion = "1.0.0-RC18-2"
val scanamoVersion = "1.0.0-M12-1"
val zioConfigVersion = "1.0.0-RC16-2"

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
  "com.typesafe.play" %% "play-json" % "2.7.4",
  "com.amazonaws" % "DynamoDBLocal" % "1.12.0" % Test,
  "com.almworks.sqlite4java" % "sqlite4java" % "1.0.392" % Test,
  "com.almworks.sqlite4java" % "libsqlite4java-osx" % "1.0.392" % Test artifacts (Artifact(
    "libsqlite4java-osx",
    "dylib",
    "dylib"
  )),
  "com.almworks.sqlite4java" % "libsqlite4java-linux-amd64" % "1.0.392" % Test artifacts (Artifact(
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
  "-Ywarn-value-discard"
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
