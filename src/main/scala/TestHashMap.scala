import scala.io.Source
import scala.util.Marshal

import java.io._

import scala.collection.mutable.HashMap

object TestHashMap {
  def main(args: Array[String]) {
    val tokens_in = new FileInputStream("/home/cc/cs294/sp13/class/cs294-ay/hw3/token_ids.ser")
    val tokens_bytes =
        Stream.continually(tokens_in.read).takeWhile(-1 !=).map(_.toByte).toArray
    val token_ids : HashMap[String, Int] =
        Marshal.load[HashMap[String, Int]](tokens_bytes)
    println("assuming -> %d\n".format(token_ids("assuming")))
    println("the -> %d\n".format(token_ids("the")))
    println("a -> %d\n".format(token_ids("a")))
    println("America -> %d\n".format(token_ids("America")))
  }
}
