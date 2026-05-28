import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object Main {
  val k = 5
  val seed = 67
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
    val genderCategories = Array("Male", "Female")
    val smokerCategories = Array("Yes", "No")
    val regionCategories = Array("Northwest", "Southwest", "Northeast", "Southeast", "Central")
    val exerciseCategories = Array("Moderate", "Low", "High")

    val data = datasetRDD()
    //drops id and annual medical cost
    val drop = data.map(row => row.slice(1, 15))

    val age = zScore(drop.map(row => row(0).toDouble)) //a
    val gender = drop.map(row => oneHot(row(1), genderCategories)) //b
    val bmi = zScore(drop.map(row => row(2).toDouble)) //c
    val children = zScore(drop.map(row => row(3).toDouble)) //d
    val smoker = drop.map(row => oneHot(row(4), smokerCategories)) //e
    val region = drop.map(row => oneHot(row(5), regionCategories)) //f
    //not z REMOVE
    //val occupation = drop.map(row => row(6))
    val annualInc = zScore(drop.map(row => row(7).toDouble)) //g
    val exLvl = drop.map(row => oneHot(row(8), exerciseCategories)) //h
    val chronDis = zScore(drop.map(row => row(9).toDouble)) //i
    val doctVis = zScore(drop.map(row => row(10).toDouble)) //j
    val hospVis = zScore(drop.map(row => row(11).toDouble)) //k
    val alcCons = zScore(drop.map(row => row(12).toDouble)) //l

    val result = age.zip(gender).zip(bmi).zip(children).zip(smoker)
      .zip(region).zip(annualInc).zip(exLvl)
      .zip(chronDis).zip(doctVis).zip(hospVis).zip(alcCons)

    result.map({ case (((((((((((a, b), c), d), e), f), g), h), i), j), k), l) =>
      Array(a) ++ b ++ Array(c, d) ++ e ++ f ++ Array( g ) ++ h ++ Array( i, j , k, l) })
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

  def oneHot(value: String, categories: Array[String]): Array[Double] = {
    categories.map(category =>
    if(value == category) 1.0 else 0.0)
  }

  def distance(a: Array[Double], b: Array[Double]): Double = {
    math.sqrt(a.zip(b).map{case (x, y) => math.pow(x - y, 2)}.sum)
  }

  def closestCentroid(a: Array[Double], b: Array[Array[Double]]): (Int, Array[Double]) = {
    //tuple w/ index of centroid and distance
    var closest = (-1, 10000.0)

    for(i <- b.indices) {
      val dist = distance(a, b(i))
      if(closest._2 > dist){
        closest = (i, dist)
      }
    }
    (closest._1, a)
  }



  def main(args: Array[String]): Unit = {
    val result = normalizeMedical()
    val centroids = result.takeSample(false, k, seed)
    //result.foreach(row => println(row.mkString(",")))

    val withCentroids = result.map(closestCentroid(_, centroids)).persist()
    withCentroids.collect().foreach(x => println(x._1 + ",       " + x._2.mkString(",")))

    withCentroids.groupByKey().collect().foreach(x => println(x._1 + ",       " + x._2.mkString(",")))

  }
}

