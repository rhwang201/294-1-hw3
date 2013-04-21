// Richard Hwang, David Huang
// CS294-1 Assignment 3

// This outputs token counts.

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

import org.apache.commons.cli.Options
import BIDMatWithHDFS._;

import scala.reflect.Manifest
import scala.collection.mutable.StringBuilder

import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

object Tokenize extends Configured with Tool {

  val text_start_tag : String = ".*<text.*>.*"
  val text_end_tag : String = ".*</text.*>.*"
  val text_single_line : String = ".*<text.*>.*</text.*>.*"

  class Map extends Mapper[LongWritable, Text, Text, IntWritable] {
    var one: IntWritable = new IntWritable(1);

    /* Emit each token from WikipediaTokenizer */
    override def map(key: LongWritable, value: Text,
          context: Mapper[LongWritable, Text, Text, IntWritable]#Context) {
      var string_text : String = value toString
      var page_text : String = get_text(string_text)

      var tok : WikipediaTokenizer =
          new WikipediaTokenizer(new StringReader(string_text))
      var charTerm : CharTermAttribute =
          tok addAttribute classOf[CharTermAttribute]

      tok.reset()
      while (tok incrementToken)
      {
        var token : String = charTerm toString ()
        context write (new Text(token), one)
      }
    }

    /* Returns text from input, or empty String if no matching tags. */
    def get_text(input: String):String = {
      var string_split : Array[String] = input split ("\n")

      var in_text : Boolean = false
      var text: StringBuilder = new StringBuilder

      string_split.foreach { line: String =>
        if (line matches text_single_line) {
          // Pass
        } else if (line matches text_start_tag)
          in_text = true
        else if (in_text)
          text append (line + "\n")
        else if (line matches text_end_tag)
          in_text = true
      }

      if (in_text)
        return ""
      else
        return text toString
    }
  }

  class Reduce extends Reducer[Text, IntWritable, Text, LongWritable] {

    val result = new LongWritable()

    override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
        context: Reducer[Text, IntWritable, Text, LongWritable]#Context) {
      var r : Long = 0
      var iter = values iterator ()
      while (iter hasNext ())
        r += iter next () get ()
      result set (r)
      var count : LongWritable = result
      context write (key, count)
    }
  }

  def run(args: Array[String]) = {
    var conf = super.getConf()
    conf set ("xmlinput.start", "<page>")
    conf set ("xmlinput.end", "</page>")

	  var job : Job = new Job(conf,"Tokenize")
		job setJarByClass(this.getClass())

		job setMapperClass classOf[Map]
		job setMapOutputKeyClass classOf[Text]
		job setMapOutputValueClass classOf[IntWritable]

		job setReducerClass classOf[Reduce]
	  job setOutputKeyClass classOf[Text]
	  job setOutputValueClass classOf[LongWritable]

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
