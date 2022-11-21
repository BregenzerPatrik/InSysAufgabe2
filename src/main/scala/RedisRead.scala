import com.google.gson.Gson

import scala.annotation.tailrec
import scala.collection.immutable

object RedisRead {
  private val gson = new Gson()
  private val jedis=RedisConnection.getConnection

  def titleByID(articleID:Long): String ={
    val redisArticle = getArticle(articleID)
    redisArticle.title
  }
  private def getArticle(articleID:Long): RedisArticle ={
    val json = jedis.get(RedisConnection.keyArticle + articleID)
    gson.fromJson(json, classOf[RedisArticle])
  }
  private def getAuthor(authorID:Long):Author={
    val jsonAuthor = jedis.get(RedisConnection.keyAuthor+authorID)
    gson.fromJson(jsonAuthor,classOf[Author])
  }
  @tailrec
  def getAuthorsFromIDS(ids: Set[Long],result:Array[Author]):Array[Author]={
    if(ids.isEmpty){
      return result
    }
    val nextAuthorID = ids.head
    val nextAuthor = getAuthor(nextAuthorID)
    getAuthorsFromIDS(ids-nextAuthorID,result:+nextAuthor)
  }
  def authors(articleID:Long): Array[Author] ={
    val article = getArticle(articleID)
    getAuthorsFromIDS(article.authors.toSet,new Array[Author](article.authors.length))
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
  private def getAllArticle(articleID:Long, allIDs: Set[String],result: List[RedisArticle]= List[RedisArticle]()):List[RedisArticle] = {
    if(allIDs.isEmpty){
      result
    }
    else {
      val thisElement = allIDs.head.toLong
      addReference(articleID,thisElement)
      val thisArticle = getArticle(thisElement)
      getAllArticle(articleID,allIDs- thisElement.toString, result:+thisArticle)
    }
  }
  @tailrec
  private def getAllArticleFromAuthor(authorID:Long, allIDs: Set[String],result: List[RedisArticle]= List[RedisArticle]()):List[RedisArticle] = {
    if(allIDs.isEmpty){
      result
    }
    else {
      val thisElement = allIDs.head.toLong
      addArticleByAuthor(authorID,thisElement)
      val thisArticle = getArticle(thisElement)
      getAllArticleFromAuthor(authorID,allIDs- thisElement.toString, result:+thisArticle)
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

  def articles(authorID: Long): List[RedisArticle]={
    val allArticleIDs = getArticleByAuthor(authorID)
    getAllArticleFromAuthor(authorID,allArticleIDs)
  }

  def mostArticles(): List[Author]={
    val most = jedis.zrange(RedisConnection.mostArticles,-1,-1)
    val mostID = most.get(0).toLong
    val author = getAuthor(mostID)
    List(author)
  }
  def distinctAuthors(): Long={
    jedis.zcard(RedisConnection.mostArticles)
  }
  def distinctAuthorsHyperLogLog(): Long={
    jedis.pfcount(RedisConnection.distinctAuthorsHyperLogLog)
  }
  def referencedBy(articleID: Long): List[RedisArticle]={
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
