
resolvers ++= Seq(
  "jenkins" at "http://repo.jenkins-ci.org/public/",
  "Era7 maven releases" at "https://s3-eu-west-1.amazonaws.com/releases.era7.com"
)

addSbtPlugin("com.localytics" % "sbt-dynamodb" % "2.0.2")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.4.1")
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.3")
addSbtPlugin("ohnosequences" % "sbt-github-release" % "0.7.0")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.0")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.11")
addSbtPlugin("org.tpolecat" % "tut-plugin" % "0.4.2")
