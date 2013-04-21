// Richard Hwang, David Huang
// CS294-1 Assignment 3

// This hashes tokens -> unique_ids

import scala.reflect.Manifest
import scala.io.Source
import scala.util.Marshal
import scala.collection.mutable.HashMap
import java.io._

object HashTokens {

  def main(args: Array[String]) {
    var token_ids : HashMap[String, Int] = new HashMap[String, Int]
    var id : Int = 0

    for (line <-
          Source.fromFile("/home/cc/cs294/sp13/class/cs294-ay/hw3/common_counts.txt") getLines) {
      var token : String = (line split ("\\s+")) (1)
      token_ids(token) = id
      id += 1
    }

    val out = 
        new FileOutputStream("/home/cc/cs294/sp13/class/cs294-ay/hw3/token_ids.ser")
    out write (Marshal dump token_ids)
    out close
  }

}
