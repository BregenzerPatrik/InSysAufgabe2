import com.google.gson.Gson

object ReddisWrite {
  private val gson = new Gson()
  private val pipeline = RedisConnection.getConnectionPipeline()

  private def addReference(articleID:Long,reference:Long)={
    pipeline.lpush(RedisConnection.referencedBy+articleID,reference.toString)
  }
  private def addArticleByAuthor(authorID:Long,articleID:Long): Unit ={
    pipeline.lpush(RedisConnection.articlesByAuthor+authorID,articleID.toString)
  }

  private def addArticleRedis(line: Line): Unit = {
      pipeline.set(RedisConnection.keyArticle+line.id.toString, gson.toJson(line))
  }
  private def addReferenceList(line: Line): Unit ={
    val references = line.references
    if (references!= null){
      references.foreach(reference=> addReference(line.id,reference))
    }
  }
  private def addAuthorRedis(author: Author): Unit ={
      pipeline.set(RedisConnection.keyAuthor + author.id.toString, gson.toJson(author))
    }
  private def addMostArticles(author: Author)={
    pipeline.zincrby(RedisConnection.mostArticles,1,author.id.toString)
  }
  private def addDistinctAuthors(author: Author):Unit={
    pipeline.sadd(RedisConnection.distinctAuthors,author.id.toString)
  }
  private def addDistinctAuthorsHyperLogLog(author: Author):Unit={
    pipeline.pfadd(RedisConnection.distinctAuthorsHyperLogLog,author.id.toString)
  }
  def addLine(line: Line):Unit={
    if(line == null){
      return
    }
    addArticleRedis(line)
    addReferenceList(line)
    if(line.authors==null){
      return
    }
    line.authors.foreach(author=>{
      addAuthorRedis(author)
      addArticleByAuthor(author.id,line.id)
      addMostArticles(author)
      addDistinctAuthors(author)
      addDistinctAuthorsHyperLogLog(author)
    })
  }
}
