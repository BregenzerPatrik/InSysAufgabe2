import com.google.gson.Gson
import redis.clients.jedis.Jedis

import java.time.LocalDateTime
import scala.annotation.tailrec
import scala.collection.immutable


object Test {
  val host = "127.0.0.1"
  val port = 6379
  val gson = new Gson()
  val jedis = new Jedis(host, port)
  val keyArticle="Article:"
  val keyAuthor="Author:"
  val referencedBy="Reference:"
  val articlesByAuthor = "AuthorsArticle:"

  def main(args: Array[String]): Unit = {
    //jedis.set("I", "Italia")
    val start = LocalDateTime.now()
    println(start)
    println(jedis.get("I"))
    println(jedis.get("5"))
    jedis.lpush("Test","Test1")
    jedis.lpush("Test","Test2")
    jedis.lpush("Test","Test3")
    println(jedis.rpop("Test"))
    println(jedis.rpop("Test"))
    println(jedis.rpop("Test"))
    println(jedis.rpop("Test"))
    //jedis.set("I","Italy")
    for (i <- Range(1, 100000)) {
      jedis.pipelined().set(i.toString, "test") //~0,2sec => pipeline funktioniert 100x Aufwand => ~18sec ~100x dauer
      //jedis.set(i.toString, "Test")//~62,36 sec
    }
    jedis.pipelined().sync()
    val end = LocalDateTime.now()
    println(end)
    flush()
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
  private def getArticle(articleID:Long): Line ={
    val json = jedis.get(keyArticle + articleID)
    gson.fromJson(json, classOf[Line])
  }
  def authors(articleID:Long): Array[Author] ={
    val line = getArticle(articleID)
    line.authors
  }
  def addArticlesByAuthor(line: Line): Unit ={
    val authors= line.authors
    if(authors!=null){
      authors.foreach(author=> addArticleByAuthor(author.id,line.id))
    }
  }
  def addArticleByAuthor(authorID:Long,articleID:Long): Unit ={
    jedis.pipelined().lpush(articlesByAuthor+authorID,articleID.toString)
  }
  def addReferenceList(line: Line): Unit ={
    val references = line.references
    if (references!= null){
      references.foreach(reference=> addReference(line.id,reference))
    }
  }
  def addReference(articleID:Long,reference:Long)={
    jedis.pipelined().lpush(referencedBy+articleID,reference.toString)
  }
  @tailrec
  def getReferencedByIDS(articleID: Long, result: Set[String]=new immutable.HashSet[String]()):Set[String] = {
    val nextElement=jedis.rpop(referencedBy+articleID)
    if(nextElement==null){
      result
    }else{
      getReferencedByIDS(articleID, result+nextElement)
    }
  }
  @tailrec
  def getAllArticle(articleID:Long, allIDs: Set[String],result: List[Line]= List[Line]()):List[Line] = {
    if(allIDs.isEmpty){
      result
    }
    else {
      val thisElement = allIDs.head.toLong
      addReference(articleID,thisElement)
      val thisLine = getArticle(thisElement)
      getAllArticle(articleID,allIDs- thisElement.toString, result:+thisLine)
    }
  }

  def referencedBy(articleID: Long): List[Line]={
    val allIDs = getReferencedByIDS(articleID:Long)
    getAllArticle(articleID,allIDs)
  }

  @tailrec
  def getAllArticleFromAuthor(authorID:Long, allIDs: Set[String],result: List[Line]= List[Line]()):List[Line] = {
    if(allIDs.isEmpty){
      result
    }
    else {
      val thisElement = allIDs.head.toLong
      addArticleByAuthor(authorID,thisElement)
      val thisLine = getArticle(thisElement)
      getAllArticleFromAuthor(authorID,allIDs- thisElement.toString, result:+thisLine)
    }
  }

  @tailrec
  def getArticleByAuthor(authorID: Long, result: Set[String]=new immutable.HashSet[String]()):Set[String] = {
    val nextElement=jedis.rpop(articlesByAuthor+authorID)
    if(nextElement==null){
      result
    }else{
      getArticleByAuthor(authorID, result+nextElement)
    }
  }
  def articles(authorID: Long): List[Line]={
    val allLineIDs = getArticleByAuthor(authorID)
    getAllArticleFromAuthor(authorID,allLineIDs)
  }
  def mostArticles(): List[Author]={
    null
  }
  def distinctAuthors(): Long={
    0
  }
  def distinctAuthorsHyperLogLog(): Long={
    0
  }
  private def flush(): Unit ={
    jedis.resetState()
    jedis.flushAll()
  }

}
