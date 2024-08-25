scalaVersion := "2.13.14"
name := "dataprocessor"
version := "1.0"

libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-library" % "2.13.14",
    "org.apache.spark" %% "spark-core" % "3.5.1",
    "org.apache.spark" %% "spark-sql" % "3.5.1",
    "io.delta" %% "delta-spark" % "3.2.0"
)

javaOptions += "-Djava.library.path=%HADOOP_HOME%\\bin"
