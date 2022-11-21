import scala.annotation.tailrec

case class RedisArticle(id:Long,
                        title:String,
                        authors:Array[Long],
                        references:Array[Long])


