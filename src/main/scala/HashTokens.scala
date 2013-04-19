// Richard Hwang, David Huang
// CS294-1 Assignment 3

// This hashes tokens -> unique_ids

import scala.reflect.Manifest
import scala.io.Source
import scala.util.Marshal
import scala.collections.mutable.HashMap
import java.io._

object HashTokens {

  def main(args: Array[String]) {
    var token_ids : HashMap = HashMap[String, Int]()
    var id : Int = 0

    for (line <- Source.fromFile("myfile.txt").getLines()) {
      var token : String = (line split (" ")) (1)
      token_ids(token) = id
      id += 1
    }

    val out = new FileOutputStream("token_ids.ser")
    out write (Marshal dump (token_ids))
    out close ()
  }

}
