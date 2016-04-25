name := "WhereIsOutlier"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe.config" % "typesafe-config" % "1.2.1",
  "com.databricks" %% "spark-csv" % "1.2.0",
  "org.scalanlp" %% "breeze" % "0.12",
  "org.scalanlp" %% "breeze-natives" % "0.12",
  "org.scalanlp" %% "breeze-viz" % "0.12",
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "org.apache.spark" %% "spark-hive" % "1.6.1",
  "org.apache.spark" %% "spark-streaming" % "1.6.1",
  //"org.apache.spark" %% "spark-streaming-kafka" % "1.6.1",
  "org.apache.spark" %% "spark-streaming-flume" % "1.6.1",
  "org.apache.spark" %% "spark-mllib" % "1.6.1",
  
  "org.apache.commons" % "commons-lang3" % "3.0",
  "org.eclipse.jetty"  % "jetty-client" % "8.1.14.v20131031",
  "com.typesafe.play" % "play-json_2.10" % "2.2.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.3.3",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.3.3",
  "org.elasticsearch" % "elasticsearch-hadoop-mr" % "2.0.0.RC1",
  "net.sf.opencsv" % "opencsv" % "2.0",
  "com.twitter.elephantbird" % "elephant-bird" % "4.5",
  "com.twitter.elephantbird" % "elephant-bird-core" % "4.5",
  "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.17",
  "mysql" % "mysql-connector-java" % "5.1.31",
  //"com.datastax.spark" %% "spark-cassandra-connector" % "1.0.0-rc5",
  //"com.datastax.spark" %% "spark-cassandra-connector-java" % "1.0.0-rc5",
  "com.github.scopt" %% "scopt" % "3.2.0",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  //"com.holdenkarau" %% "spark-testing-base" % "0.0.1" % "test" 
  "com.thesamet" %% "kdtree" % "1.0.4"
)

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public")
)