name := "advertising_analysis"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "com.typesafe" % "config" % "1.3.1",
  "com.mkorneev" %% "spark-excel" % "0.8.3-SNAPSHOT"
)

