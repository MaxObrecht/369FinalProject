import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object Main {
  def datasetRDD(): RDD[Array[String]] = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Medical")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/medical_insurance_cost_dataset.csv")
    val header = lines.first()

    lines
      .filter(_!=header)
      .map(line => line.split(","))
  }

  def normalizeMedical() = {
    val data = datasetRDD()
    //drops id and annual medical cost
    val drop = data.map(row => row.slice(1, 15))

    val age = zScore(drop.map(row => row(0).toDouble))
    //shouldn't z, needs "one-hot-encode"
    val gender = drop.map(row => row(1))
    val bmi = zScore(drop.map(row => row(2).toDouble))
    val children = zScore(drop.map(row => row(3).toDouble))
    //not z
    val smoker = drop.map(row => row(4))
    //not z
    val region = drop.map(row => row(5))
    //not z
    val occupation = drop.map(row => row(6))
    val annualInc = zScore(drop.map(row => row(7).toDouble))
    //not z
    val exLvl = drop.map(row => row(8))
    val chronDis = zScore(drop.map(row => row(9).toDouble))
    val doctVis = zScore(drop.map(row => row(10).toDouble))
    val hospVis = zScore(drop.map(row => row(11).toDouble))
    val alcCons = zScore(drop.map(row => row(12).toDouble))

    val result = age.zip(gender).zip(bmi).zip(children).zip(smoker)
      .zip(region).zip(occupation).zip(annualInc).zip(exLvl)
      .zip(chronDis).zip(doctVis).zip(hospVis).zip(alcCons)

    result.map({ case ((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m) =>
      (a, b, c, d, e, f, g, h, i, j , k, l, m) })
  }

  def mean(data: RDD[Double]): Double = {
    val result = data.aggregate( (0.0,0) ) (
      (x,y) => (x._1 + y, x._2 + 1),
      (x,y) => (x._1 + y._1, x._2 + y._2))
    result._1 / result._2
  }

  def std(data: RDD[Double]): Double = {
    val avg = mean(data)
    val sumSquareDiffs = data.fold(0.0)((total, n) => total + math.pow(n - avg, 2))
    math.sqrt(sumSquareDiffs / data.count)
  }

  def zScore(data: RDD[Double]): RDD[Double] = {
    val avg = mean(data)
    val compStd = std(data)
    data.map(n => (n - avg) / compStd)
  }

  def main(args: Array[String]): Unit = {
    val result = normalizeMedical()
    result.foreach(println)


  }
}

