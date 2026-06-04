import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object Main {
  //random seed
  val seed = 67
  //calculated k from Silhouette Score
  val k = 8
  //constant maxIteration for K-means
  val maxIter = 25

  //Initial RDD from medical insurance cost dataset csv
  def normalRDD(): RDD[Array[String]] = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Medical")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/medical_insurance_cost_dataset.csv")
    val header = lines.first()

    lines
      .filter(_ != header)
      .map(line => line.split(","))
  }

  //Normalized RDD
  def normalizeMedical(): RDD[Array[Double]] = {
    //COMMENTED OUT GENDER, SMOKER, AND EXERCISE CATEGORIES AND REMOVED FROM ZIPPING

    val normalrdd = normalRDD()


    val data = normalrdd.persist()
    val id = data.map(row => row(0).drop(3).toDouble)
    val age = zScore(data.map(row => row(1).toDouble)) //a
    //val gender = zScore(data.map(row => {if (row(2) == "Male") 1.0 else 0.0})) //b
    val bmi = zScore(data.map(row => row(3).toDouble)) //c
    val children = zScore(data.map(row => row(4).toDouble)) //d
    //val smoker = zScore(data.map(row => {if (row(5) == "Yes") 1.0 else 0.0})) //e
    val annualInc = zScore(data.map(row => row(8).toDouble)) //g
    val chronDis = zScore(data.map(row => row(10).toDouble)) //i
    val doctVis = zScore(data.map(row => row(11).toDouble)) //j
    val hospVis = zScore(data.map(row => row(12).toDouble)) //k
    val alcCons = zScore(data.map(row => row(13).toDouble)) //l
    val cost = data.map(row => row(15).toDouble)

    //ZIPPING HERE
    val result = id.zip(age).zip(bmi).zip(children).zip(annualInc)
      .zip(chronDis).zip(doctVis).zip(hospVis).zip(alcCons).zip(cost)

    result.map { case (((((((((id, age), bmi), children), annualInc),
    chronDis), doctVis), hospVis), alcCons), cost) =>
      Array(id, age, bmi, children, annualInc, chronDis, doctVis, hospVis, alcCons, cost) //++
        //Array(bmi, children) ++
        //Array(annualInc) ++
        //Array(chronDis, doctVis, hospVis, alcCons, cost)
    }
  }

  //mean helper method, calculates mean for given RDD of Double
  def mean(data: RDD[Double]): Double = {
    val result = data.aggregate((0.0, 0))(
      (x, y) => (x._1 + y, x._2 + 1),
      (x, y) => (x._1 + y._1, x._2 + y._2))
    result._1 / result._2
  }

  //Standard deviation helper method, calculates std for given RDD of Double
  def std(data: RDD[Double]): Double = {
    val avg = mean(data)
    val (sumSq, count) = data.aggregate((0.0, 0))(
      (acc, n) => (acc._1 + math.pow(n - avg, 2), acc._2 + 1),
      (a, b)   => (a._1 + b._1, a._2 + b._2)
    )
    math.sqrt(sumSq / count)
  }

  //z score helper, calculates z score for given RDD of Double
  def zScore(data: RDD[Double]): RDD[Double] = {
    val persisted = data.persist()
    val avg = mean(persisted)
    val compStd = std(persisted)
    persisted.map(n => (n - avg) / compStd)
  }

  //Euclidian distance helper, given two points represented as Array of Double, gives a Double distance between the two
  def distance(a: Array[Double], b: Array[Double]): Double = {
    math.sqrt(a.zip(b).map { case (x, y) => math.pow(x - y, 2) }.sum)
  }

  //given a point 'a' and an array of points, finds the closest point to 'a'
  def closestCentroid(a: Array[Double], b: Array[Array[Double]]): (Int, Array[Double]) = {
    //tuple w/ index of centroid and distance
    var closest = (-1, Double.PositiveInfinity)

    for (i <- b.indices) {
      val dist = distance(a.slice(1, a.length-1), b(i).slice(1, b(i).length-1))
      if (closest._2 > dist) {
        closest = (i, dist)
      }
    }
    (closest._1, a)
  }

  //given a list of point pairs, returns the list of all the average distances between points within the cluster
  def intraClusterDist(pairs: RDD[((Int, Array[Double]), (Int, Array[Double]))]) = {
    pairs.filter { case ((q1, v1), (q2, v2)) =>
            v1(0) != v2(0) && q1 == q2}
      .map { case ((q1, v1), (q2, v2)) =>
        //slice to remove id and cost from cluster consideration
        (v1(0) , (distance(v1.slice(1, v1.length-1), v2.slice(1, v2.length-1)), 1))
      }
      .reduceByKey { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      .mapValues { case (sum, count) => sum / count }
  }

  //given a list of point pairs, returns the list of minimum distance between points and another cluster
  def nearestClusterDist(pairs: RDD[((Int, Array[Double]), (Int, Array[Double]))]) = {
    val distBetweenClusters = pairs.filter({case ((q1, v1), (q2, v2)) =>
        v1(0) != v2(0) && q1 != q2})
    .map { case ((q1, v1), (q2, v2)) =>
      //slice to remove id and cost from cluster consideration
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

  //Silhouette score helper
  def silhouetteScore(intra: Double, near: Double): Double = {
    (near - intra) / Math.max(intra, near)
  }

  //calculates full SilhouetteScore, the average of all the SilhouetteScores
  def finalSilhouetteScore(a : RDD[(Int, Array[Double])]) : Double = {
    val pairs = a.cartesian(a).filter { case ((q1, v1), (q2, v2)) =>
      v1(0) != v2(0)
    }.persist()

    val intra = intraClusterDist(pairs)
    val nearest = nearestClusterDist(pairs)

    val silhouetteScores = nearest.leftOuterJoin(intra)
      .map { case (id, (nearestDist, maybeIntra)) =>
        maybeIntra match {
          case Some(intraDist) => silhouetteScore(intraDist, nearestDist)
          case None => 0.0
        }
      }

    val (sum, count) = silhouetteScores.aggregate((0.0, 0L))(
      { case ((s, c), x) => (s + x, c + 1) },
      { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }
    )

    pairs.unpersist()

    sum / count
  }

  //Given an input RDD of Array[Double], calculates Silhouette Scores for a list of k
  def getK(input : RDD[Array[Double]]): Unit = {

    //val K = (2 to 100 by 2).toList
    val K = List(2, 3, 5, 8, 10, 15, 20)

    var bestK = -1
    var bestSil = Double.NegativeInfinity

    for(kVal <- K) {
      val withCentroids = kMeans(input, kVal)

      val finalSillScore = finalSilhouetteScore(withCentroids)

      if(finalSillScore > bestSil && finalSillScore <= 1)
        {
          bestK = kVal
          bestSil = finalSillScore
        }
      println(kVal + "    " + finalSillScore)
      withCentroids.unpersist()
    }
    println("BEST " + bestK + "    " + bestSil)
  }

  //Calls getK on several different combination of attributes
  def kCaller(): Unit = {
    //(id, age, bmi, children, annualInc, chronDis, doctVis, hospVis, alcCons, cost)
    val normalize = normalizeMedical().persist()

    //age, bmi, children, chronic disease, alcohol consumption
    val healthOnly = normalize.map( row =>
      Array(
        row(0),  // id
        row(1),  // age
        row(2),  // bmi
        row(3),  // children
        row(5),  // chronic disease
        row(8), // alcohol consumption
        row(9)  // cost
      )
    ).persist()
    //doctor visits, hospital visits, chronic disease
    val utilizationOnly = normalize.map (row =>
      Array (
        row(0), //id
        row(5), //chron
        row(6), //doctor
        row(7), //hospt
        row(9)  //cost
      )
    ).persist()
    // age, children, annual income
    val demoOnly = normalize.map (row =>
      Array (
        row(0), //id
        row(1), //age
        row(3), //children
        row(4), //anual income
        row(9) //cost
      )
    ).persist()
    //age, bmi, chronic disease, hospital visits
    val strongestOnly = normalize.map (row =>
      Array (
        row(0), //id
        row(1), //age
        row(2), //bmi
        row(5), //chronic
        row(7), //hospital visits
        row(9) //cost
      )
    ).persist()

    //forces caching
    normalize.count()
    healthOnly.count()
    utilizationOnly.count()
    demoOnly.count()
    strongestOnly.count()

    println("BASELINE")
    getK(normalize)
    println("HEALTHONLY")
    getK(healthOnly)
    println("UTILIZATION")
    getK(utilizationOnly)
    println("DEMOGRAPHICS")
    getK(demoOnly)
    println("STRONGESTATTRIBUTES")
    getK(strongestOnly)
  }

  //recompute centroids helper for kMeans, new centroids are the averages of the points belonging to the old centroid
  def recomputeCentroids(withCentroids: RDD[(Int, Array[Double])], oldCentroids: Array[Array[Double]]): Array[Array[Double]] = {
    val newCentroids = withCentroids.map({case (c, row) =>
      val features = row.slice(1, row.length-1)
      (c, (features, 1))
    })
      .reduceByKey({case ((features1, c1), (features2, c2)) => {
          val sumFeat = features1.zip(features2).map({case (x, y) => x + y})
          val count = c1 + c2
        (sumFeat, count)
        }
      })
      .mapValues({ case (sumFeat, count) => {
        val avg = sumFeat.map(_/count)
        Array(0.0) ++ avg ++ Array(0.0)
      }}).collect().toMap

    //it's possible that some old centroids are empty
    oldCentroids.indices.map { i =>
      // If a cluster gets no points, keep the old centroid
      newCentroids.getOrElse(i, oldCentroids(i))
    }.toArray
  }

  //given an input RDD and a k, produces an RDD of clusters based on the k-means algorithm
  def kMeans(input :  RDD[Array[Double]], k : Int):  RDD[(Int, Array[Double])] = {
    var centroids = input.takeSample(false, k, seed)
    var withCentroids: RDD[(Int, Array[Double])] = null

    for (_ <- 1 to maxIter) {
      if (withCentroids != null) {
        withCentroids.unpersist()
      }

      withCentroids = input
        .map(row => closestCentroid(row, centroids))
        .persist()

      centroids = recomputeCentroids(withCentroids, centroids)
    }
    withCentroids
  }

  def main(args: Array[String]): Unit = {
    //Silhouette Score calculation
//    kCaller();

    //Attributes clustered on:
    //doctor visits, hospital visits, chronic disease
    val utilizationOnly = normalizeMedical().map (row =>
      Array (
        row(0), //id
        row(5), //chron
        row(6), //doctor
        row(7), //hospt
        row(9)  //cost
      )
    )

    //saves clusters to clusters directory
    val clusters = kMeans(utilizationOnly, k)
    clusters.map({case (cluster, row) =>
      cluster + "," + row.mkString(",")
    }).saveAsTextFile("src/clusters")
  }
}

