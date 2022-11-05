
resolvers ++= Seq(
  "jenkins" at "https://repo.jenkins-ci.org/public/",
  "Era7 maven releases" at "https://s3-eu-west-1.amazonaws.com/releases.era7.com"
)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.0.0")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.11")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
//addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.6")
addSbtPlugin("com.github.duhemm" % "sbt-errors-summary" % "0.6.5")

libraryDependencies += "com.sun.activation" % "javax.activation" % "1.2.0"
