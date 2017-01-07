import sbtrelease.ReleaseStateTransformations._
import ohnosequences.sbt.GithubRelease
import org.kohsuke.github.GHRelease

enablePlugins(JavaAppPackaging, BuildInfoPlugin)

buildInfoPackage := "dynamite"

scalaVersion := "2.12.1"

mainClass in Compile := Some("dynamite.Main")

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "fastparse" % "0.4.2",
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.76",
  "jline" % "jline" % "2.14.3",
  "com.lihaoyi" %% "fansi" % "0.2.3",
  "com.github.scopt" %% "scopt" % "3.5.0",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "com.typesafe.play" %% "play-json" % "2.6.0-M1" % Test
)

dynamoDBLocalVersion := "2016-05-17"

startDynamoDBLocal := startDynamoDBLocal.dependsOn(compile in Test).value
test in Test := (test in Test).dependsOn(startDynamoDBLocal).value
testOptions in Test += dynamoDBLocalTestCleanup.value
testOnly in Test := (testOnly in Test).dependsOn(startDynamoDBLocal).value
testQuick in Test := (testQuick in Test).dependsOn(startDynamoDBLocal).value

dynamoDBLocalPort := new java.net.ServerSocket(0).getLocalPort

testOptions in Test += Tests.Setup(() => 
  System.setProperty("dynamodb.local.port", dynamoDBLocalPort.value.toString)
)

dynamoDBLocalDownloadDir := baseDirectory.value / "dynamodb-local"

SbtScalariform.scalariformSettings 

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
  "-Ywarn-adapted-args", 
  "-Ywarn-inaccessible",
  "-Ywarn-dead-code"
)

lazy val checkVersionNotes = taskKey[Unit](
	"Checks that the notes for the next version are present to avoid build failures."
)

checkVersionNotes := {
  val notesFile = baseDirectory.value / "notes" / (version.value.stripSuffix("-SNAPSHOT") +".markdown")
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
  releaseStepTask(ghreleaseGetCredentials),
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

