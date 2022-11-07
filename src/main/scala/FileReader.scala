import com.google.gson.Gson

import java.io.File
import java.util.Scanner
import java.time.LocalDateTime

object FileReader {

  def main(args: Array[String]): Unit = {
    val file = new File("./dblp.v12.json/dblp.v12.json")
    val scanner = new Scanner(file)
    val start = LocalDateTime.now()
    println("Beginn: "+start)
    scanner.nextLine() //erste Zeile, die nur aus [ besteht überspringen
    val gson = new Gson()
    var lineNumber=1
    while (scanner.hasNextLine) {
      if(lineNumber % 489408==0){
        val timestamp = LocalDateTime.now()
        println("Zeilennummer: "+lineNumber+"Zeitpunkt: "+timestamp)
      }
      val json = scanner.nextLine().replaceAll("[^\\p{ASCII}]","?")
      if (json.startsWith(",") && !json.equals("]")) {
        val line = gson.fromJson(json.substring(1), classOf[Line])
        readline(line)
      } else if(!json.equals("]")){
        val line = gson.fromJson(json, classOf[Line])
        readline(line)
      }
      lineNumber += 1
    }
    DB.addForeignKeys()
    val end = LocalDateTime.now()
    println("Ende: " + end)
  }


  def readline(line: Line): Unit = {
    DB.addArticle(line)
    DB.addAuthors(line.authors)
    DB.addArticleAuthorsReference(line.id, line.authors)
    DB.addArticleArticlesReference(line.id,line.references)
  }
}
