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

  def normalRDD(): RDD[Array[String]] = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Medical")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/medical_insurance_cost_dataset.csv")
    val header = lines.first()

    lines
      .filter(_ != header)
      .map(line => line.split(","))
  }

  def normalizeMedical(): RDD[Array[Double]] = {
    //COMMENTED OUT GENDER, SMOKER, AND EXERCISE CATEGORIES AND REMOVED FROM ZIPPING

    val genderCategories = Array("Male", "Female")
    val smokerCategories = Array("Yes", "No")
    //val regionCategories = Array("Northwest", "Southwest", "Northeast", "Southeast", "Central")
    //val exerciseCategories = Array("Moderate", "Low", "High")

    val data = normalRDD()
    //drops id and annual medical cost
    //val drop = data.map(row => row.slice(1, 15))

    val id = data.map(row => row(0).drop(3).toDouble)
    val age = zScore(data.map(row => row(1).toDouble)) //a
    val gender = data.map(row => {if (row(2) == "Male") 1.0 else 0.0}) //b
    val bmi = zScore(data.map(row => row(3).toDouble)) //c
    val children = zScore(data.map(row => row(4).toDouble)) //d
    val smoker = data.map(row => {if (row(5) == "Yes") 1.0 else 0.0}) //e
    //val region = drop.map(row => oneHot(row(5), regionCategories)) //f
    //not z REMOVE
    //val occupation = drop.map(row => row(6))
    val annualInc = zScore(data.map(row => row(8).toDouble)) //g
    //val exLvl = data.map(row => oneHot(row(9), exerciseCategories)) //h
    val chronDis = zScore(data.map(row => row(10).toDouble)) //i
    val doctVis = zScore(data.map(row => row(11).toDouble)) //j
    val hospVis = zScore(data.map(row => row(12).toDouble)) //k
    val alcCons = zScore(data.map(row => row(13).toDouble)) //l
    //val insurance dropped
    val cost = data.map(row => row(15).toDouble)

    //ZIPPING HERE
    val result = id.zip(age).zip(gender).zip(bmi).zip(children).zip(smoker).zip(annualInc)
      .zip(chronDis).zip(doctVis).zip(hospVis).zip(alcCons).zip(cost)

    result.map { case (((((((((((id, age), gender), bmi), children), smoker), annualInc),
    chronDis), doctVis), hospVis), alcCons), cost) =>
      Array(id, age) ++
        Array(gender) ++
        Array(bmi, children) ++
        Array(smoker) ++
        Array(annualInc) ++
        Array(chronDis, doctVis, hospVis, alcCons, cost)
    }
  }

  def mean(data: RDD[Double]): Double = {
    val result = data.aggregate((0.0, 0))(
      (x, y) => (x._1 + y, x._2 + 1),
      (x, y) => (x._1 + y._1, x._2 + y._2))
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
      if (value == category) 1.0 else 0.0)
  }

  def distance(a: Array[Double], b: Array[Double]): Double = {
    math.sqrt(a.zip(b).map { case (x, y) => math.pow(x - y, 2) }.sum)
  }

  def closestCentroid(a: Array[Double], b: Array[Array[Double]]): (Int, Array[Double]) = {
    //tuple w/ index of centroid and distance
    var closest = (-1, 10000.0)

    for (i <- b.indices) {
      val dist = distance(a.slice(1, a.length-1), b(i).slice(1, b(i).length-1))
      if (closest._2 > dist) {
        closest = (i, dist)
      }
    }
    (closest._1, a)
  }

  def fillCentroid(r: RDD[Array[Double]]) = {
    val centroids = r.takeSample(false, k, seed)
    r.map(closestCentroid(_, centroids))
  }

  def intraClusterDist(a: RDD[(Int, Array[Double])]): RDD[(Double, Double)] = {
    val pairs = a.cartesian(a).filter { case ((q1, v1), (q2, v2)) =>
      v1(0) != v2(0) && q1 == q2
    }

    pairs.map { case ((q1, v1), (q2, v2)) =>
        (v1(0) , (distance(v1.slice(1, v1.length-1), v2.slice(1, v2.length-1)), 1))
      }
      .reduceByKey { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      .mapValues { case (sum, count) => sum / count }
  }

  def nearestClusterDist(a: RDD[(Int, Array[Double])]): RDD[(Double, Double)] = {
    val pairs = a.cartesian(a).filter { case ((q1, v1), (q2, v2)) =>
      v1(0) != v2(0) && q1 != q2
    }

    val distBetweenClusters = pairs.map { case ((q1, v1), (q2, v2)) =>
      ((v1(0) , q2) ,(distance(v1.slice(1,v1.length-1), v2.slice(1,v2.length-1)), 1))
    }
      .reduceByKey { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      .mapValues { case (sum, count) => sum / count }

    distBetweenClusters
      .map {
        case ((id1, otherCluster), avgDist) =>
          (id1, avgDist)
      }
      .reduceByKey(math.min)
  }

  def silhouetteScore(intra: Double, near: Double): Double = {
    (near - intra) / Math.max(intra, near)
  }

  def getK(): Unit = {
    val result = normalizeMedical()

//    val K = List(5,10,15,20,30,40,50,75,100,150,200)
val K = (1 to 100).toList

    for(i <- 0 to K.length-1) {
      val centroids = result.takeSample(false, K(i), seed)

      val withCentroids = result.map(closestCentroid(_, centroids)).persist()
      val silhouetteScores = intraClusterDist(withCentroids).join(nearestClusterDist(withCentroids))
        .map {case (id, (intra, nearest)) =>
          silhouetteScore(intra, nearest)}
      val finalSillScore = silhouetteScores.sum() / silhouetteScores.count()
      println(K(i) + "    " + finalSillScore)
    }
  }

  def kMeans(): Unit = {
    //initialize
    val result = normalizeMedical()
    var centroids = result.takeSample(false, k, seed)

    //loop
    //assign to closest centroids
    val withCentroids = result.map(closestCentroid(_, centroids)).persist()

    //recompute centroids
    //take average of each centroid, that becomes new centroid
    //centroids = ...
  }

  def main(args: Array[String]): Unit = {
    val result = normalizeMedical()
    val centroids = result.takeSample(false, k, seed)
//  //result.foreach(row => println(row.mkString(",")))
//
    val withCentroids = result.map(closestCentroid(_, centroids)).persist()
    withCentroids.map(x => x._1 + ", " + x._2.mkString(",")).saveAsTextFile("data/ModifiedOneHot")
    withCentroids.collect().foreach(x => println(x._1 + ",       " + x._2.mkString(",")))

//
//    // Silhouette Score code
//    val indexed = addIds(withCentroids).persist()
//
//    val silhouetteScores = intraClusterDist(indexed).join(nearestClusterDist(indexed))
//      .map {case (id, (intra, nearest)) =>
//      silhouetteScore(intra, nearest)}
//
//    val finalSillScore = silhouetteScores.sum() / silhouetteScores.count()
//    //
//
//    println(finalSillScore)

    //withCentroids.groupByKey().collect().foreach(x => println(x._1 + ",       " + x._2.mkString(",")))

    //getK()

  }
}

