import com.google.gson.Gson
import redis.clients.jedis.Jedis

import java.time.LocalDateTime
import scala.annotation.tailrec
import scala.collection.immutable


object Test {


  def main(args: Array[String]): Unit = {
    flush()
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















}
