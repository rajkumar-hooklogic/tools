name := "scala-tools"

organization := "rajkumar.org"

version := "0.1"

scalaVersion := "2.12.3"

publishMavenStyle := true


// - Dependencies ---
libraryDependencies ++= Seq(
  "org.scala-lang"            % "scala-reflect"       % "2.12.2",
  "com.typesafe.akka"         % "akka-actor_2.12"     % "2.5.3", 
  "org.json4s"                % "json4s-native_2.12"  % "3.5.2",
  "commons-io"                % "commons-io"          % "2.5",
  "org.apache.httpcomponents" % "httpclient"          % "4.5.2",
  "org.apache.tomcat.embed"   % "tomcat-embed-core"   % "8.5.0",
  "org.apache.tomcat.embed"   % "tomcat-embed-logging-juli" % "8.5.0",
  "javax.mail"                % "mail"                % "1.4.7",
  "com.amazonaws"             % "aws-java-sdk"        % "1.11.154",

  // DBs
  "com.aerospike"             % "aerospike-client"    % "4.0.3",
  "com.couchbase.client"      % "java-client"         % "2.4.6",
  "io.reactivex"              % "rxscala_2.12"        % "0.26.5",
  "redis.clients"             % "jedis"               % "2.9.0",
  "postgresql"                % "postgresql"          % "9.1-901-1.jdbc4",
  "com.maxmind.geoip2"        % "geoip2"              % "2.7.0",
  "com.microsoft.sqlserver"   % "mssql-jdbc"          % "6.1.0.jre8",

  // Logging
  "ch.qos.logback"            % "logback-classic"     % "1.1.8",

  // Testing
  "org.scalatest"             %% "scalatest"          % "3.0.3"     % "test"
)

