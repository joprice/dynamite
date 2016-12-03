import sbtrelease.ReleaseStateTransformations._
import ohnosequences.sbt.GithubRelease
import org.kohsuke.github.GHRelease

enablePlugins(JavaAppPackaging, BuildInfoPlugin)

buildInfoPackage := "dynamite"

scalaVersion := "2.11.8"

mainClass in Compile := Some("dynamite.Main")

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "fastparse" % "0.4.2",
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.8",
  "org.scalatest" %% "scalatest" % "2.2.6" % Test,
  "com.typesafe.play" %% "play-json" % "2.5.3" % Test,
  "jline" % "jline" % "2.14.2",
  "com.lihaoyi" %% "fansi" % "0.1.3",
  "com.github.scopt" %% "scopt" % "3.5.0",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
)

dynamoDBLocalVersion := "2016-03-01"

startDynamoDBLocal <<= startDynamoDBLocal.dependsOn(compile in Test)
test in Test <<= (test in Test).dependsOn(startDynamoDBLocal)
testOptions in Test <+= dynamoDBLocalTestCleanup
testOnly in Test <<= (testOnly in Test).dependsOn(startDynamoDBLocal)

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

