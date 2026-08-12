// Name of the package
name := "spark-warehouse"

// Version of our package
version := "1.0-SNAPSHOT"

// Version of Scala
scalaVersion := "2.12.18"


libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "3.5.5" % "provided",
	"org.apache.spark" %% "spark-sql"  % "3.5.5" % "provided",
	// PostgreSQL JDBC driver
	"org.postgresql" % "postgresql" % "42.7.8",
	// Typesafe Config
	"com.typesafe" % "config" % "1.4.3"
)


// // Spark dependencies are provided by the Spark runtime 
Compile / run := Defaults.runTask( 
	Compile / fullClasspath, 
	Compile / run / mainClass, 
	Compile / run / runner 
).evaluated



