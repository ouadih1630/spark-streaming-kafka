name := "Test"
version := "1.0"
scalaVersion := "2.13.8"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % "3.4.1" % "provided",
                            "org.apache.spark" %% "spark-core" % "3.4.1" % "provided")  //  "3.4.1" : Spark version