import com.google.gson.Gson
import redis.clients.jedis.Jedis

import java.time.LocalDateTime


object Test {
  val host = "127.0.0.1"
  val port = 6379
  val jedis = new Jedis(host, port)
  val gson = new Gson()
  val keyArticle="Article:"
  val keyAuthor="Author:"

  def main(args: Array[String]): Unit = {
    println(jedis.get("I"))
    //jedis.set("I", "Italia")
    val start = LocalDateTime.now()
    println(start)
    for (i <- Range(1, 100000)) {
      jedis.pipelined().set(i.toString, "test") //~0,2sec => pipeline funktioniert 100x Aufwand => ~18sec ~100x dauer
      //jedis.set(i.toString, "Test")//~62,36 sec
    }
    val end = LocalDateTime.now()
    println(end)
    jedis.close()
  }

  def addArticleRedis(line: Option[Line]): Unit = {
    line match
    {
      case Some(value) =>jedis.pipelined().set(keyArticle+value.id.toString, gson.toJson(value))
      case _ => println("ERROR")
    }
  }
  def addAuthorRedis(author: Option[Author]): Unit ={
    author match {
      case Some(value) => jedis.pipelined().set(keyAuthor + value.id.toString, gson.toJson(value))
      case _ => println("ERROR")
    }
  }
  def titleByID(articleID:Long): String ={
    val line = getArticle(articleID)
    line.title
  }
  def getArticle(articleID:Long): Line ={
    val json = jedis.get(keyArticle + articleID)
    gson.fromJson(json, classOf[Line])
  }
  def authors(articleID:Long): Array[Author] ={
    val line = getArticle(articleID)
    line.authors
  }
}
