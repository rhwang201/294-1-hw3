// Richard Hwang, David Huang
// CS294-1 Assignment 3

// TODO TupleWritable isn't working

import java.io.IOException
import java.util._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
//import org.apache.hadoop.mapred.join.TupleWritable
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
      var tok : WikipediaTokenizer = new WikipediaTokenizer(new StringReader(value.toString()))
      var charTerm : CharTermAttribute = tok addAttribute classOf[CharTermAttribute]

      tok reset ()
      while (tok incrementToken ())
      {
        var token : String = charTerm buffer () toString ()
        context write (new Text(token), one)
      }
    }
  }

  class Reduce extends Reducer[Text, IntWritable, Text, TupleWritable] {
    override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
        context: Reducer[Text, IntWritable, Text, TupleWritable]#Context) {
      var count : Int = 0
      var valsIter = values.iterator()
      while (valsIter.hasNext())
        count += 1
      var toWrite : Array[Writable] = 
          Array(new IntWritable(key hashCode ()), new IntWritable(count))
      var gonnaWrite : TupleWritable = new TupleWritable(toWrite)
      context write (key, gonnaWrite)
    }
  }

  def run(args: Array[String]) = {
    var conf = super.getConf()
	  var job : Job = new Job(conf,"Tokenize")
		job setJarByClass(this.getClass())

		job setMapperClass classOf[Map]
		job setMapOutputKeyClass classOf[Text]
		job setMapOutputValueClass classOf[IntWritable]

		job setReducerClass classOf[Reduce]
	  job setOutputKeyClass classOf[Text]
	  job setOutputValueClass classOf[TupleWritable]

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
