import com.google.gson.Gson

import scala.annotation.tailrec
import scala.collection.immutable

object RedisRead {
  val gson = new Gson()
  val jedis=RedisConnection.getConnection()

  def titleByID(articleID:Long): String ={
    val line = getArticle(articleID)
    line.title
  }
  private def getArticle(articleID:Long): Line ={
    val json = jedis.get(RedisConnection.keyArticle + articleID)
    gson.fromJson(json, classOf[Line])
  }
  private def getAuthor(authorID:Long):Author={
    val jsonAuthor = jedis.get(RedisConnection.keyAuthor+authorID)
    gson.fromJson(jsonAuthor,classOf[Author])
  }

  def authors(articleID:Long): Array[Author] ={
    val line = getArticle(articleID)
    line.authors
  }
  @tailrec
  private def getReferencedByIDS(articleID: Long, result: Set[String]=new immutable.HashSet[String]()):Set[String] = {
    val nextElement=jedis.rpop(RedisConnection.referencedBy+articleID)
    if(nextElement==null){
      result
    }else{
      getReferencedByIDS(articleID, result+nextElement)
    }
  }
  @tailrec
  private def getAllArticle(articleID:Long, allIDs: Set[String],result: List[Line]= List[Line]()):List[Line] = {
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
  @tailrec
  private def getAllArticleFromAuthor(authorID:Long, allIDs: Set[String],result: List[Line]= List[Line]()):List[Line] = {
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
  private def getArticleByAuthor(authorID: Long, result: Set[String]=new immutable.HashSet[String]()):Set[String] = {
    val nextElement=jedis.rpop(RedisConnection.articlesByAuthor+authorID)
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
  /*@tailrec
  private def getMostArticles(result: Set[String]=new immutable.HashSet[String]()):Set[String]={
    val most = jedis.zrangeWithScores(RedisConnection.mostArticles,-1,-1)
    most.get(0)[3]
    if(result.isEmpty)

  }*/
  def mostArticles(): List[Author]={
    val most = jedis.zrange(RedisConnection.mostArticles,-1,-1)
    val mostLong = most.get(0).toLong
    val author = getAuthor(mostLong)
    List(author)
  }
  def distinctAuthors(): Long={
    jedis.scard(RedisConnection.distinctAuthors)
  }
  def distinctAuthorsHyperLogLog(): Long={
    jedis.pfcount(RedisConnection.distinctAuthorsHyperLogLog)
  }
  def referencedBy(articleID: Long): List[Line]={
    val allIDs = getReferencedByIDS(articleID:Long)
    getAllArticle(articleID,allIDs)
  }

  //notwendige Writes während der reads (die nicht in der Pipeline ausgeführt werden)

  private def addReference(articleID:Long,reference:Long)={
    jedis.lpush(RedisConnection.referencedBy+articleID,reference.toString)
  }
  private def addArticleByAuthor(authorID:Long,articleID:Long): Unit ={
    jedis.lpush(RedisConnection.articlesByAuthor+authorID,articleID.toString)
  }
}
