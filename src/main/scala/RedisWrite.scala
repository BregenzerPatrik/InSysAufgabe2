import com.google.gson.Gson

object RedisWrite {
  private val gson = new Gson()
  private val pipeline = RedisConnection.getConnectionPipeline

  private def addReference(articleID: Long, reference: Long) = {
    pipeline.lpush(RedisConnection.referencedBy + reference, articleID.toString)
  }

  private def addArticleByAuthor(authorID: Long, articleID: Long): Unit = {
    pipeline.lpush(RedisConnection.articlesByAuthor + authorID, articleID.toString)
  }

  private def addArticleRedis(redisArticle: RedisArticle): Unit = {
    pipeline.set(RedisConnection.keyArticle + redisArticle.id.toString, gson.toJson(redisArticle))
  }

  private def addReferenceList(redisArticle: RedisArticle): Unit = {
    val references = redisArticle.references
    if (references != null) {
      references.foreach(reference => addReference(redisArticle.id, reference))
    }
  }

  private def addAuthorRedis(author: Author): Unit = {
    pipeline.set(RedisConnection.keyAuthor + author.id.toString, gson.toJson(author))
  }

  private def addMostArticles(authorID: Long) = {
    pipeline.zincrby(RedisConnection.mostArticles, 1, authorID.toString)
  }

  private def addDistinctAuthorsHyperLogLog(authorID: Long): Unit = {
    pipeline.pfadd(RedisConnection.distinctAuthorsHyperLogLog, authorID.toString)
  }

  def addLine(redisArticle: RedisArticle, authors: Array[Author]): Unit = {
    if (redisArticle == null) {
      return
    }
    addArticleRedis(redisArticle)
    addReferenceList(redisArticle)
    if (redisArticle.authors == null) {
      return
    }
    redisArticle.authors.foreach(authorID => {
      addArticleByAuthor(authorID, redisArticle.id)
      addMostArticles(authorID)
      addDistinctAuthorsHyperLogLog(authorID)
    })
    if (authors != null) {
      authors.foreach(author => addAuthorRedis(author))
    }
  }

  def sync(): Unit = {
    pipeline.sync()
  }

  def closePipeline(): Unit = {
    pipeline.close()
  }
}
