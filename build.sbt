name := "CodingExercise_AM"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.6"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies ++= sparkDependencies
