// Richard Hwang, David Huang
// CS294-1 Assignment 3

// This outputs token counts.

import java.io.IOException
import java.util._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.util._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import BIDMat.MatFunctions._
import BIDMat.{IMat,FMat}
import scala.reflect.Manifest
import java.io.PrintWriter
import java.io.StringReader
import org.apache.commons.cli.Options
import BIDMatWithHDFS._;

import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

object Tokenize extends Configured with Tool {

  class Map extends Mapper[LongWritable, Text, Text, IntWritable] {
    var one: IntWritable = new IntWritable(1);

    /* Emit each token from WikipediaTokenizer */
    override def map(key: LongWritable, value: Text,
          context: Mapper[LongWritable, Text, Text, IntWritable]#Context) {
      var string_text : String = value toString ()
      var string_split : Array[String] = string_text split ("\n")
      // Check for bad splits
      if (string_split(0).trim == "<page>") {
        var tok : WikipediaTokenizer =
            new WikipediaTokenizer(new StringReader(string_text))
        var charTerm : CharTermAttribute =
            tok addAttribute classOf[CharTermAttribute]

        tok reset ()
        while (tok incrementToken ())
        {
          var token : String = charTerm toString ()
          context write (new Text(token), one)
        }
      }
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

	  var job : Job = new Job(conf,"bb gerl")
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
