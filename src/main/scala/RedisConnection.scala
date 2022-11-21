import redis.clients.jedis.{Jedis, Pipeline}

object RedisConnection {
  private val host = "127.0.0.1"
  private val port = 6379
  private val jedis = new Jedis(host, port,50000)
  val keyArticle="Article:"
  val keyAuthor="Author:"
  val referencedBy="Reference:"
  val articlesByAuthor = "AuthorsArticle:"
  val mostArticles = "MostArticles:"
  val distinctAuthors = "DistinctAuthors:"
  val distinctAuthorsHyperLogLog = "DistinctAuthorsHyperLogLog:"

  def getConnectionPipeline:Pipeline={
    jedis.pipelined()
  }
  def getConnection:Jedis={
    jedis
  }

  def flush(): Unit ={
    jedis.flushAll()
  }
}
