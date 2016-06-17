
enablePlugins(JavaAppPackaging)

scalaVersion := "2.11.8"

version := "0.0.7"

mainClass in Compile := Some("dynamite.Repl")

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "fastparse" % "0.3.7",
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.10.73",
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

GithubRelease.repo := "joprice/dynamite" 

//GithubRelease.draft := true

GithubRelease.releaseAssets := Seq((packageBin in Universal).value)

