ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "InsysAufgabe2"
  )
// https://mvnrepository.com/artifact/redis.clients/jedis
libraryDependencies += "redis.clients" % "jedis" % "4.3.1"
