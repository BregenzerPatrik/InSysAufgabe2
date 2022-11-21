import scala.annotation.tailrec

object Conversions {

  def line2RedisArticle(line: Line): RedisArticle ={
    if(line.authors!=null){
      val length = line.authors.length
      val idArray = new Array[Long](length)
      val authorIds = getIDArray(line.authors,idArray)
      return RedisArticle(line.id, line.title,authorIds,line.references)
    }
    RedisArticle(line.id, line.title,null,line.references)
  }

  @tailrec
  def getIDArray(authors:Array[Author],result:Array[Long]):Array[Long]={
    if(authors.isEmpty){
      return result
    }
    val nextAuthor=authors.head
    getIDArray(authors.filter(!_.equals(nextAuthor)) ,result:+nextAuthor.id)
  }

}
