// Richard Hwang, David Huang
// CS294-1 Assignment 3

// Generate Bag of Words and target vector

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
import org.apache.commons.cli.Options
import BIDMatWithHDFS._;
import scala.util.Marshal
import scala.io.Source
import java.io._
import scala.collection.mutable.HashMap

import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

object Bag extends Configured with Tool {

  val num_features : Int = 10000

  class Map extends Mapper[LongWritable, Text, IntWritable, MatIO] {
    var label : IntWritable = new IntWritable();
    var matIO : MatIO = new MatIO()

    val tokens_in = new FileInputStream("out") // TODO check rel path
    val bytes =
        Stream.continually(tokens_in.read).takeWhile(-1 !=).map(_.toByte).toArray
    val token_ids : HashMap[String, Int] =
        Marshal.load[HashMap[String, Int]](bytes)

    val cats_in = new FileInputStream("out") // TODO check rel path
    val bytes =
        Stream.continually(cats_in.read).takeWhile(-1 !=).map(_.toByte).toArray
    val subcats : HashSet[String] =
        Marshal.load[HashSet[String]](bytes)

    val icol_row : BIDMat.IMat = icol(0 to num_features-1)
    val icol_col : BIDMat.IMat = ones(num_features, 1)

    val cat_pattern : String = ".*Category:.*"

    /* Emit each [y X] */
    override def map(key: LongWritable, value: Text,
          context: Mapper[LongWritable, Text, IntWritable, MatIO]#Context) {
      var string_text : String = value toString ()
      var string_split : Array[String] = string_text split ("\n")
      var category : Int = class_label(string_split)

      // Check for bad splits
      if (string_split(0).trim == "<page>" || category == -1) {
        // Initialize tokenizer
        var tok : WikipediaTokenizer =
            new WikipediaTokenizer(new StringReader(string_text))
        var charTerm : CharTermAttribute =
            tok addAttribute classOf[CharTermAttribute]

        var cur_counts : BIDMat.IMat = zeros(num_features, 1)

        // For each token
        tok reset ()
        while (tok incrementToken ())
        {
          var token : String = charTerm toString ()

          if (token_ids contains (token)) {
            var index : Int = token_ids(token)
            cur_counts(index, 1) = cur_counts(index, 1) + 1
          }
        }

        var feature_vect : BIDMat.SMat =
            sparse(icol_row, icol_col, cur_counts, num_features, 1)
        matIO.mat = feature_vect

        // Only write if we found a class
        if (category != -1) {
          label set (category)
          context write (label, matIO)
        }
      }
    }

    /* Returns the class label for text and subcats, -1 if cannot
     * find Category. */
    def class_label(text: String):Int = {
      var match : bool = false
      text.foreach { line =>
        // Category line
        if (line matches (cat_pattern)) {
          // Check if this is in subcats
          var cat : String = (line split (":"))(1) dropRight (2)
          if (subcats contains (cat)) {
            return true
          }
          match = true
        }
      }
      if !match {
        return -1
      } else {
        return 0
      }
    }
  }

  class Reduce extends Reducer[IntWritable, MatIO, IntWritable, MatIO] {
    override def reduce(key: IntWritable, values: java.lang.Iterable[MatIO],
        context: Reducer[IntWritable, MatIO, IntWritable, MatIO]#Context) {
      var valsIter = values iterator ()
      while (valsIter hasNext ())
        context write (key, valsIter.next)
    }
  }

  def run(args: Array[String]) = {
    var conf = super.getConf()
	  var job : Job = new Job(conf,"BagOfWords")
		job setJarByClass(this.getClass())

		job setMapperClass classOf[Map]
		job setMapOutputKeyClass classOf[IntWritable]
		job setMapOutputValueClass classOf[MatIO]

		job setReducerClass classOf[Reduce]
	  job setOutputKeyClass classOf[IntWritable]
	  job setOutputValueClass classOf[MatIO]

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
