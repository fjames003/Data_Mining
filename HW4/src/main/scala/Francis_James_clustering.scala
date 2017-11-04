import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.PriorityQueue
import sys.process._

object Francis_James_clustering {
  def main(args: Array[String]): Unit = {
    require(args.length <= 2, "Please provide an input file and the number of clusters")

    // Using SparkConf to avoid deprecation warning.
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Clustering")
    val sc = new SparkContext(conf)

    // (sepal length, sepal width, petal length, petal width, iris class)
    val data = sc.textFile(args(0)).map{line =>
      val split = line.split(",")
      (split(0).toDouble, split(1).toDouble, split(2).toDouble, split(3).toDouble, split(4))
    }
    val data_with_index = data.zipWithIndex().map{ case (k, v) => (v, k)}
    // Function to return the euclidean distance between two tuple5.
    def euclid_dist(point_a: (Double, Double, Double, Double, _), point_b: (Double, Double, Double, Double, _)): Double = {
              math.sqrt(math.pow(point_a._1 - point_b._1, 2)
                      + math.pow(point_a._2 - point_b._2, 2)
                      + math.pow(point_a._3 - point_b._3, 2)
                      + math.pow(point_a._4 - point_b._4, 2))
    }

    // Compute	pairwise	dist.	of	all	points
    val distances = data_with_index.cartesian(data_with_index).filter{ case (flower1, flower2) => flower1._1 < flower2._1}
        .map{ case (flower1, flower2) => ((flower1._1, flower2._1), euclid_dist(flower1._2, flower2._2))}

    // Build	priority	queue
    val que = scala.collection.mutable.PriorityQueue.empty(Ordering[Double].reverse)

    val dist_que = distances.map { case (pair, dist) =>
      scala.collection.mutable.PriorityQueue(dist)
    }.reduce(_++_)


    dist_que.take(5).foreach(println(_))


    // 2. Apply the hierarchical algorithm to find clusters. Use Euclidean distance
    //        start from merging the first two closest points, then the next closest, etc.
    //        the coordinate of centroid is defined as the average of that of all the points in the cluster.
    // 3. Assign each final cluster a name by choosing the most frequently occurring class label of the examples in the cluster.
    // 4. Count the number of data points that were put in each cluster.
    // 5. Find the number of data points that were put in clusters in which they didn’t belong (based on having a
    // different class label than the cluster name).
  }
}
