// Richard Hwang, David Huang
// CS294-1 Assignment 3

// Generate Bag of Words and target vector
// TODO <text> and tok.CATEGORY

import java.util._
import java.io._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.util._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.commons.cli.Options

import BIDMat.MatFunctions._
import BIDMat.{IMat,FMat,SMat}
import BIDMatWithHDFS._;

import scala.reflect.Manifest
import scala.util.Marshal
import scala.io.Source
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.JavaConversions._

import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

object Bag extends Configured with Tool {

  val num_features : Int = 10000

  class Map extends Mapper[LongWritable, Text, IntWritable, MatIO] {
    var label : IntWritable = new IntWritable();
    var matIO : MatIO = new MatIO()

    var fs : FileSystem = FileSystem.get(new Configuration())

    var tokens_in : InputStream = fs.open(new Path("/cs294_1/hw3-ay/token_ids.ser"))
    val tokens_bytes =
        Stream.continually(tokens_in.read).takeWhile(-1 !=).map(_.toByte).toArray
    val token_ids : HashMap[String, Int] =
        Marshal.load[HashMap[String, Int]](tokens_bytes)

    val cats_in : InputStream = fs.open(new Path("/cs294_1/hw3-ay/subcats.ser"))
    val cats_bytes =
        Stream.continually(cats_in.read).takeWhile(-1 !=).map(_.toByte).toArray
    val subcats : HashSet[String] =
        Marshal.load[HashSet[String]](cats_bytes)

    var icol_row : BIDMat.IMat = icol(0 to num_features-1)
    val icol_col : BIDMat.IMat = iones(num_features, 1)

    val cat_pattern : String = ".*Category:.*"

    /* Emit each [y X] */
    override def map(key: LongWritable, value: Text,
          context: Mapper[LongWritable, Text, IntWritable, MatIO]#Context) {
      icol_row = icol(0 to num_features-1)

      var string_text : String = value toString ()
      var category : Int = class_label(string_text)

      if (category != -1) {
        // Initialize tokenizer
        var tok : WikipediaTokenizer =
            new WikipediaTokenizer(new StringReader(string_text))


        var charTerm : CharTermAttribute =
            tok addAttribute classOf[CharTermAttribute]

        var cur_counts : BIDMat.IMat = izeros(num_features, 1)

        // For each token
        tok reset ()
        while (tok incrementToken ())
        {
          var token : String = charTerm toString ()

          if (token_ids contains (token)) {
            var index : Int = token_ids(token)
            cur_counts(index, 0) = cur_counts(index, 0) + 1
          }
        }

        var feature_vect : BIDMat.SMat =
            sparse(icol_row, icol_col, cur_counts, num_features, 1)
        matIO.mat = feature_vect

        label set (category)
        context write (label, matIO)
      }
    }

    /* Returns the class label for text and subcats, -1 if cannot
     * find Category. */
    def class_label(text : String):Int = {
      var found_match : Boolean = false
      var lines = text split ("\n")

      lines.foreach { line =>
        // Category line
        if (line matches (cat_pattern)) {
          // Check if this is in subcats
          var cat : String = (line split (":"))(1) dropRight (2)
          if (subcats contains (cat))
            return 1
          found_match = true
        }
      }
      if (!found_match)
        return -1
      else
        return 0
    }
  }

  class Reduce extends Reducer[IntWritable, MatIO, IntWritable, Text] {

    var toWrite: Text= new Text()

    override def reduce(key: IntWritable, values: java.lang.Iterable[MatIO],
        context: Reducer[IntWritable, MatIO, IntWritable, Text]#Context) {
      var valsIter = values iterator ()
      while (valsIter hasNext ()) {
        var matIO = valsIter.next.mat match {
          case sMat: SMat => sMat;
        }
        var sBuilder =
          (new StringBuilder /: matIO.data) ((soFar, newFloat) =>
            soFar.append(newFloat + " "))
        toWrite set (sBuilder toString ())
        context write (key, toWrite)
      }
    }
  }

  def run(args: Array[String]) = {
    var conf = super.getConf()
    conf set ("xmlinput.start", "<text xml:space=\"preserve\">")
    conf set ("xmlinput.end", "</text>")

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
