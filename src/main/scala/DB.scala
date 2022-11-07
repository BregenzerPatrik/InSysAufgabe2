import java.sql.DriverManager
object DB {
  val URL = "jdbc:h2:./demo"
  val connection = DriverManager.getConnection(URL)
  val authorInsert = "insert into Author values(?,?,?)"
  val articleInsert = "insert into Article values(?,?,?,?,?,?,?,?,?,?,?)"
  val articleAuthorReference = "insert into ArticleAuthor values(?,?)"
  val articleArticleReference = "insert into ArticleReference values(?,?)"
  val authorInsertStatement = connection.prepareStatement(authorInsert)
  val articleInsertStatement = connection.prepareStatement(articleInsert)
  val articleAuthorReferenceInsertStatement = connection.prepareStatement(articleAuthorReference)
  val articleArticleReferenceInsertStatement = connection.prepareStatement(articleArticleReference)

  def main(args: Array[String]): Unit = {
    createTables()
  }

  def createTables(): Unit = {
    val statement = connection.createStatement()
    val createAutor =
      """create table IF NOT EXISTS Author(
        |id Long primary key,
        |name varchar(100),
        |org varchar(280))""".stripMargin
    statement.execute(createAutor)
    val createArtikel =
      """create table IF NOT EXISTS Article(
        |id Long primary key,
        |title varchar(570) not null,
        |`year` int not null,
        |n_citation int not null,
        |page_start varchar(20),
        |page_end varchar(20),
        |doc_type varchar(30),
        |publisher varchar(330),
        |volume varchar(30),
        |issue varchar(30),
        |doi varchar(160) not null)""".stripMargin
    statement.execute(createArtikel)
    val createArtikelAutor =
      """create table IF NOT EXISTS ArticleAuthor(
        |Author Long references Author,
        |Article Long references Article,
        |primary Key(Author,Article))""".stripMargin
    statement.execute(createArtikelAutor)
    val createArtikelReferenz =
      """create table IF NOT EXISTS ArticleReference(
        |Article Long,
        |Reference Long,
        |CONSTRAINT Selbstreferenz CHECK(Article<>Reference),
        |primary Key(Article,Reference))""".stripMargin
    statement.execute(createArtikelReferenz)
    statement.close()
    closeConnection()
  }

  def addArticle(line: Line): Unit = {
    articleInsertStatement.setLong(1, line.id)
    articleInsertStatement.setString(2, line.title)
    articleInsertStatement.setInt(3, line.year)
    articleInsertStatement.setInt(4, line.n_citation)
    articleInsertStatement.setString(5, line.page_start)
    articleInsertStatement.setString(6, line.page_end)
    articleInsertStatement.setString(7, line.doc_type)
    articleInsertStatement.setString(8, line.publisher)
    articleInsertStatement.setString(9, line.volume)
    articleInsertStatement.setString(10, line.volume)
    articleInsertStatement.setString(11, line.doi)
    try {
      articleInsertStatement.executeUpdate()
    } catch {
      case e: java.sql.SQLTimeoutException => printException(e.getMessage)
      case e: java.sql.SQLException => printException(e.getMessage)
    }
  }

  def addAuthor(author: Author): Unit = {
    authorInsertStatement.setLong(1, author.id)
    authorInsertStatement.setString(2, author.name)
    authorInsertStatement.setString(3, author.org)
    try {
      authorInsertStatement.executeUpdate()
    } catch {
      case e: java.sql.SQLTimeoutException => printException(e.getMessage)
      case e: java.sql.SQLException => printException(e.getMessage)
    }
  }

  def printException(message: String): Unit = {
    if (message.startsWith("Wert"))
      println(message)
  }

  def addAuthors(authors: Array[Author]): Unit = {
    if (authors != null) {
      authors.foreach(x => addAuthor(x))
    }
  }

  def addArticleAuthorReference(articleId: Long, author: Author): Unit = {
    articleAuthorReferenceInsertStatement.setLong(1, author.id)
    articleAuthorReferenceInsertStatement.setLong(2, articleId)
    try {
      articleAuthorReferenceInsertStatement.executeUpdate()
    } catch {
      case e: java.sql.SQLTimeoutException => printException(e.getMessage)
      case e: java.sql.SQLException => printException(e.getMessage)
    }
  }

  def addArticleAuthorsReference(articleId: Long, authors: Array[Author]): Unit = {
    if (authors != null) {
      authors.foreach(author => addArticleAuthorReference(articleId, author))
    }
  }

  def addArticleArticlesReference(articleId: Long, articles: Array[Long]): Unit = {
    if (articles != null) {
      articles.foreach(reference => addArticleArticleReference(articleId, reference))
    }
  }

  def addArticleArticleReference(articleId: Long, referenceArticle: Long): Unit = {
    articleArticleReferenceInsertStatement.setLong(1, articleId)
    articleArticleReferenceInsertStatement.setLong(2, referenceArticle)
    try {
      articleArticleReferenceInsertStatement.executeUpdate()
    } catch {
      case e: java.sql.SQLTimeoutException => printException(e.getMessage)
      case e: java.sql.SQLException => printException(e.getMessage)
    }
  }

  def closeConnection(): Unit = {
    authorInsertStatement.close()
    articleInsertStatement.close()
    articleArticleReferenceInsertStatement.close()
    articleAuthorReferenceInsertStatement.close()
    connection.close()
  }

  def addForeignKeys(): Unit = {
    val statement = connection.createStatement()
    val foreignKey1 = "Alter Table ArticleReference add Foreign Key (Article) references Article(id)"
    val foreignKey2 = "Alter Table ArticleReference add Foreign Key (Reference) references Article(id)"
    statement.execute(foreignKey1)
    statement.execute(foreignKey2)
    statement.close()
    closeConnection()
  }
}