// Richard Hwang, David Huang
// CS294-1 Assignment 3

// Train Logistic Regression Model

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

import BIDMat.MatFunctions._
import BIDMat.{Mat,IMat,FMat,SMat}
import BIDMat.SciFunctions._
import BIDMat.Solvers._
import BIDMat.Plotting._
import BIDMatWithHDFS._;

import scala.reflect.Manifest
import scala.util.Random

import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

object LogisticRegression extends Configured with Tool {

  /* Trains a logistic regression model */
  class Map extends Mapper[LongWritable, Text, IntWritable, MatIO] {

    val rand = new Random()

    override def map(key: LongWritable, value: Text,
          context: Mapper[LongWritable, Text, IntWritable, MatIO]#Context) {
        var data_labels : SMat = load_mat(value toString ())

        var n = 0
        var gamma = 0
        var reg_l = 0
        var beta : FMat = sgd(data_labels, n, gamma, reg_l)

        context write (null, null) // TODO
    }

    /* Loads data_labels */
    def load_mat(text: String):SMat = {
      // SPlit by newlines
      //   split by spaces
      return sprand(5,5,0.5) // TODO
    }

    /* Performs stochastic gradient descent */
    def sgd(data_labels: SMat, n: Int, gamma: Double, reg_l: Double):FMat = {
      // Initialize beta
      val n : Int = data_labels.ncols
      val d : Int = data_labels.nrows
      var beta : FMat = zeros(d-1, 1)
      var i : Int = 0

      for (i <- 0 to n-1) {
        var sample_i : Int = rand.nextInt(n)
        var point_rand : SMat = data_labels(?, sample_i) // TODO assuming transposed
        beta = beta + gamma * (grad_b(beta, point_rand) - reg_l * beta)
      }
      return beta
    }

    /* Calculates the gradient of the log likelihood function */
    def grad_b(beta: FMat, point: SMat):FMat = {
      
      return zeros(5,5) // TODO
    }

    /* Calculates P(Y=1|x) = 1 / (1 + exp(-beta^T * x)) */
    def logistic(beta: FMat, x: SMat):FMat = {
      var p: Double = 1 / ( 1 + exp( -(beta.t * x)(0, 0) ) )
      return p
    }
  }

  /* Averages models */
  class Reduce extends Reducer[IntWritable, MatIO, MatIO, IntWritable] {
    override def reduce(key: IntWritable, values: java.lang.Iterable[MatIO],
        context: Reducer[IntWritable, MatIO, MatIO, IntWritable]#Context) {
      var count : Int = 0
      var valsIter = values.iterator()
      while (valsIter.hasNext())
        count += 1
      var toWrite : Array[Writable] =
          Array(new IntWritable(key hashCode ()), new IntWritable(count))
      var gonnaWrite : ArrayWritable =
          new ArrayWritable(classOf[IntWritable], toWrite)
      context write (null, null) // TODO
    }
  }

  def run(args: Array[String]) = {
    var conf = super.getConf()
	  var job : Job = new Job(conf,"Tokenize")
		job setJarByClass(this.getClass())

		job setMapperClass classOf[Map]
		job setMapOutputKeyClass classOf[IntWritable]
		job setMapOutputValueClass classOf[MatIO]

		job setReducerClass classOf[Reduce]
	  job setOutputKeyClass classOf[MatIO]
	  job setOutputValueClass classOf[IntWritable]

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
