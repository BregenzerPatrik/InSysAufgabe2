import redis.clients.jedis.Jedis

object Test {
  val host = "127.0.0.1"
  val port = 6379

  def main(args: Array[String]): Unit = {
    val jedis = new Jedis(host, port)
    println(jedis.get("I"))
    //jedis.set("I", "Italia")

  }
}
