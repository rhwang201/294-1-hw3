import java.io._
import java.util.Scanner
import scala.collection.mutable.HashSet
import scala.util.Marshal
import scala.io.Source

object HashCategories {

  def main(args: Array[String]) {
    val CATEGORIES:String = "GRAPH_THEORY"
    val input_file:File = new File(System.getProperty("user.dir")+"/categories/"+CATEGORIES)
    var sc:Scanner = new Scanner(input_file)
    var categories:HashSet[String] = new HashSet()
    while (sc.hasNext()) {
      var token:String = sc.next().replaceAll(",", "")
      categories += token
    }

    val out = new FileOutputStream("out")
    out.write(Marshal.dump(categories))
    out.close
  }
}
