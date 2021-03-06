// Richard Hwang, David Huang
// CS294-1 Assignment 3

// This sorts by count

import java.io.IOException
import java.util._
import java.io.PrintWriter
import java.io.StringReader

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.util._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scala.reflect.Manifest

import org.apache.commons.cli.Options
import BIDMatWithHDFS._;

import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

object SortByCount extends Configured with Tool {

  class Map extends Mapper[LongWritable, Text, LongWritable, Text] {
    var count : LongWritable = new LongWritable()
    var token : Text = new Text()

    override def map(key: LongWritable, value: Text,
          context: Mapper[LongWritable, Text, LongWritable, Text]#Context) {
      var text : Array[String] = value toString () split ("\\s+")

      if (text.length == 2) {
        var token_str : String = text(0)
        var count_i : Long = text(1).toLong

        token set (token_str)
        count set (count_i)

        context write (count, token)
      }
    }
  }

  class Reduce extends Reducer[LongWritable, Text, LongWritable, Text] {

    override def reduce(key: LongWritable, values: java.lang.Iterable[Text],
        context: Reducer[LongWritable, Text, LongWritable, Text]#Context) {
      var iter = values.iterator()
      while (iter hasNext)
        context write (key, iter next)
    }
  }

  def run(args: Array[String]) = {
    var conf = super.getConf()
    conf set ("mapred.max.split.size", "10000000")

	  var job : Job = new Job(conf,"Sort token counts")
		job setJarByClass(this.getClass())

		job setMapperClass classOf[Map]
		job setMapOutputKeyClass classOf[LongWritable]
		job setMapOutputValueClass classOf[Text]

		job setReducerClass classOf[Reduce]
	  job setOutputKeyClass classOf[LongWritable]
	  job setOutputValueClass classOf[Text]

  	FileInputFormat.addInputPath(job, new Path(args(0)))
  	FileOutputFormat.setOutputPath(job, new Path(args(1)))
  	job waitForCompletion(true) match {
      case true => 0
      case false => 1
		}
  }

  def main(args: Array[String]) {
    var  c : Configuration = new Configuration()
    var res : Int = ToolRunner.run(c, this, args)
    System.exit(res);
  }

}
