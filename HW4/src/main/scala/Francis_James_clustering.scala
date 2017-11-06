import org.apache.spark.{SparkConf, SparkContext}

object Francis_James_clustering {
  def main(args: Array[String]): Unit = {
//    require(args.length >= 2, "Please provide an input file and the number of clusters")

    // Function to return the euclidean distance between two tuple5.
    def euclid_dist(point_a: (Double, Double, Double, Double, _), point_b: (Double, Double, Double, Double, _)):
    Double = {
              math.sqrt(math.pow(point_a._1 - point_b._1, 2)
                      + math.pow(point_a._2 - point_b._2, 2)
                      + math.pow(point_a._3 - point_b._3, 2)
                      + math.pow(point_a._4 - point_b._4, 2))
    }

    def compute_centroid(cluster: Array[(Double, Double, Double, Double, String)]):
    (Double, Double, Double, Double, String) = {
      val dim = cluster.length.toDouble
      cluster.reduce{(cluster_1, cluster_2) => (
        (cluster_1._1 + cluster_2._1) / dim,
        (cluster_1._2 + cluster_2._2) / dim,
        (cluster_1._3 + cluster_2._3) / dim,
        (cluster_1._4 + cluster_2._4) / dim,
        "")}
    }

    // my_order takes a distance between two clusters and returns the distance to order by...
    def my_order(flower: (Double, (Array[(Double, Double, Double, Double, String)],
                                   Array[(Double, Double, Double, Double, String)]))) = {
      flower._1
    }

    // Using SparkConf to avoid deprecation warning.
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Clustering")
    val sc = new SparkContext(conf)

    // (sepal length, sepal width, petal length, petal width, iris class)
    val data = sc.textFile("../given_files/Iris/iris_data.csv").map{line =>
      val split = line.split(",")
      (split(0).toDouble, split(1).toDouble, split(2).toDouble, split(3).toDouble, split(4))
    }
//    val cluster_size = args(1).toInt
    val cluster_size = 3
    val data_with_index = data.zipWithIndex().map{ case (k, v) => (v, k)}

    // Compute	pairwise	dist.	of	all	points
    val distances = data_with_index.cartesian(data_with_index).filter{ case (flower1, flower2) =>
      flower1._1 < flower2._1
    }.map{ case (flower1, flower2) =>
      (euclid_dist(flower1._2, flower2._2), (Array(flower1._2), Array(flower2._2)))
    }
    // Distance_array is now an array where each entry is a distance between two clusters (array of single items)
    val distance_array = distances.collect()

    // Keep track of clusters
    var clusters = data.map{Array(_)}.collect()

    // Use priorityQueue to hold distances in order to be more efficient
    val dist_que = new scala.collection.mutable.PriorityQueue
      [(Double, (Array[(Double, Double, Double, Double, String)],
                 Array[(Double, Double, Double, Double, String)])
        )]()(Ordering.by(my_order).reverse)

    // Add all distances to PriorityQueue for singleton clusters
    distance_array.foreach(flower => dist_que.enqueue(flower))

    // Keep going until number of clusters is equal to cluster size provided by user
    while (clusters.length > cluster_size) {
      // Find two closest clusters
      val (cluster1, cluster2) = dist_que.dequeue()._2
      // Remove the two clusters from our list of clusters
      clusters = clusters.filterNot(flower => flower.sameElements(cluster1) || flower.sameElements(cluster2))

      val new_cluster = cluster1 ++ cluster2
      val new_centroid = compute_centroid(new_cluster)
      val new_queue = dist_que.dequeueAll.filter{ case (_, (c1, c2)) => ! c1.sameElements(cluster1) && ! c1.sameElements(cluster2) && ! c2.sameElements(cluster1) && ! c2.sameElements(cluster2)}
      new_queue.foreach(dist_que.enqueue(_))
      for (cluster <- clusters) {
        val c_centroid = compute_centroid(cluster)
        val dist = euclid_dist(c_centroid, new_centroid)
        dist_que.enqueue((dist, (cluster, new_cluster)))
      }
      // Add merged clusters back into list of clusters
      clusters = clusters.union(Array(new_cluster))
      // Recompute cluster distances and load into dist_que
//      dist_que.clear()
//
//      val cluster_with_index = clusters.zipWithIndex.map{ case (k, v) => (v, k)}
//      for (cluster_1 <- cluster_with_index; cluster_2 <- cluster_with_index if cluster_1._1 < cluster_2._1) {
//        val centroid1 = compute_centroid(cluster_1._2)
//        val centroid2 = compute_centroid(cluster_2._2)
//        val dist = euclid_dist(centroid1, centroid2)
//        dist_que.enqueue((dist, (cluster_1._2, cluster_2._2)))
//      }
    }
    // 3. Assign each final cluster a name by choosing the most frequently occurring class label of the examples in the cluster.
    def display_clusters(clusters: Array[Array[(Double, Double, Double, Double, String)]]): Unit = {
      val counts = Array[(Int, Int, Int)]()
      clusters.zipWithIndex.foreach{ case (cluster, index) =>
        counts.union(Array(0,0,0))
        cluster.foreach{flower =>
          flower._5 match {
            case value if value == "Iris-setosa" =>
              val current_count = counts(index)
              counts.update(index, (current_count._1 + 1, current_count._2, current_count._3))
            case value if value == "Iris-virginica" =>
              val current_count = counts(index)
              counts.update(index, (current_count._1, current_count._2 + 1, current_count._3))
            case value if value == "Iris-versicolor" =>
              val current_count = counts(index)
              counts.update(index, (current_count._1, current_count._2, current_count._3 + 1))
          }
        }
      }
      // Need to count number in wrong cluster as well.
      var number_wrong = 0
      for (cluster <- clusters) {
        val index = clusters.indexOf(cluster)
        val cluster_count = counts(index)
        cluster_count match {
          case (setosa, virginica, versicolor) if setosa >= virginica && setosa >= versicolor =>
            val name = "Iris-setosa"
            println("cluster " + name)
            cluster.foreach{ flower =>
              if (!flower._5.equals(name)) {
                number_wrong += 1
              }
              println(flower)
            }
            println("Number of points in this cluster: " + cluster.length)
          case (setosa, virginica, versicolor) if virginica > setosa && virginica > versicolor =>
            val name = "Iris-virginica"
            println("cluster " + name)
            cluster.foreach{ flower =>
              if (!flower._5.equals(name)) {
                number_wrong += 1
              }
              println(flower)
            }
            println("Number of points in this cluster: " + cluster.length)
          case (setosa, virginica, versicolor) if versicolor > setosa && versicolor > virginica =>
            val name = "Iris-versicolor"
            println("cluster " + name)
            cluster.foreach{ flower =>
              if (!flower._5.equals(name)) {
                number_wrong += 1
              }
              println(flower)
            }
            println("Number of points in this cluster: " + cluster.length)
        }
      }
      println("Number of points assigned to wrong cluster: " + number_wrong)
    }
    display_clusters(clusters)
  }
}
