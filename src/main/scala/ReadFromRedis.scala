object ReadFromRedis {

  val test = RedisConnection.getConnection

  def main(args: Array[String]): Unit = {
    read()
  }


  def read(): Unit = {
    val titleByTD = RedisRead.titleByID(2133234465L) //müsste "EducaTableware: computer-augmented tableware to enhance the eating experiences" sein
    println("TitelByID: " + titleByTD)
    val authors = RedisRead.authors(2133235227L) // müsste [{"name":"H. Hussmann","org":"Commun. Networks, Tech. Hochschule Aachen, Germany","id":2343654773}] sein
    authors.foreach(author => {
      println("AuthorsByArticle: " + author)
    })
    val articles = RedisRead.articles(2526292829L) //müsste Artikel 2133235420 beinhalten
    articles.foreach(article => {
      println("ArticlesByAuthors: " + article)
    })
    val referencedBy = RedisRead.referencedBy(1506263111L) //müsste 2133235507 beinhalten
    referencedBy.foreach(reference => {
      println("ReferencedBy: " + reference)
    })
    val mostArticles = RedisRead.mostArticles()
    println("MostArticles: " + mostArticles)
    val distinctAuthors = RedisRead.distinctAuthors()
    println("DistinctAuthors: " + distinctAuthors)
    val distinctAuthorsHyperLogLog = RedisRead.distinctAuthorsHyperLogLog()
    println("DistinctAuthorsHyperLogLog: " + distinctAuthorsHyperLogLog)


  }
}
