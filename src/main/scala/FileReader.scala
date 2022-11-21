import com.google.gson.Gson

import java.io.File
import java.util.Scanner
import java.time.LocalDateTime
import scala.annotation.tailrec

object FileReader {
  val syncCycle = 2000

  def main(args: Array[String]): Unit = {
    RedisConnection.flush()
    val file = new File("../../dblp.v12.json")
    val scanner = new Scanner(file)
    val start = LocalDateTime.now()
    println("Beginn: " + start)
    scanner.nextLine() //erste Zeile, die nur aus [ besteht überspringen
    val gson = new Gson()
    nextLine(scanner,gson,1)
    RedisWrite.sync()
    RedisWrite.closePipeline()
    val end = LocalDateTime.now()
    println("End: " + end)
  }

  @tailrec
  private def nextLine(scanner: Scanner, gson: Gson, numberInCycle: Int): Unit = {
    if (!scanner.hasNextLine) {
      return
    }
    val json = scanner.nextLine().replaceAll("[^\\p{ASCII}]", "?")
    if (json.startsWith(",") && !json.equals("]")) {
      val line = gson.fromJson(json.substring(1), classOf[Line])
      RedisWrite.addLine(Conversions.line2RedisArticle(line),line.authors)
    } else if (!json.equals("]")) {
      val line = gson.fromJson(json, classOf[Line])
      RedisWrite.addLine(Conversions.line2RedisArticle(line),line.authors)
    }
    if ((numberInCycle % syncCycle) == 0) {
      println("Next " + syncCycle + " Lines")
      RedisWrite.sync()
    }
    nextLine(scanner, gson, (numberInCycle % syncCycle) + 1)
  }
}
