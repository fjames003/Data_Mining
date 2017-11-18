import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Betweenness {
//  Construct the graph
    //  Each node represents a user. Each edge is generated in following way. In rating.csv, count the
    //    number of times that two users rated the same movie. If the number of times is greater or
    //    equivalent to three times, there is an edge between two users.
    def main(args: Array[String]): Unit = {
      val t1 = System.nanoTime
      require(args.length >= 3, "Please provide input file and both output locations")
      // Using SparkConf to avoid deprecation warning.
      val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("Girvan-Newman")
      val sc = new SparkContext(conf)

      val vertex_table = sc.textFile(args(0)).mapPartitionsWithIndex{
        (index, iterator) => if (index == 0) iterator.drop(1) else iterator
      }.map{_.split(",") match {
        case Array(user, movie, _, _) => (user.toInt, Set(movie.toInt))
      }}.reduceByKey(_++_).zipWithIndex().map{ case (k,v) => (v,k)}

      val vertex_table_without_set: RDD[(VertexId, Int)] = vertex_table.map{ case (id, (user, _)) =>
        (id, user)
      }

//      val edge_table = vertex_table.cartesian(vertex_table).filter{ case (node1, node2) =>
//        node1._1 < node2._1
//      }.map{ case ((id1, (_, user1_set)), (id2, (_, user2_set))) =>
//        val num_corated = user1_set.intersect(user2_set).size
//        if (num_corated >= 3) Edge(id1, id2, 1) else Edge(id1, id2, 0)
//      }
//      val edge_table = vertex_table.cartesian(vertex_table).filter{ case (node1, node2) =>
//        node1._1 < node2._1
//      }
//      val edge_table = vertex_table_without_set.cartesian(vertex_table_without_set).filter{ case (node1, node2) =>
//        node1._1 < node2._1
//      }.map{ case ((id1, _), (id2, _)) => (id1, id2)}

      var pairs = new Array[Edge[Int]](0)
      val vertex_array = vertex_table.collect()
      for (user1 <- vertex_array; user2 <- vertex_array if user1._1 < user2._1) {
        val num_corated = user1._2._2.intersect(user2._2._2).size
        if (num_corated >= 3) {
          pairs = Edge(user1._1, user2._1, 1) +: pairs
        }
      }

      val default_user = -1
      val edge_rdd = sc.parallelize(pairs)

      val user_graph = Graph(vertex_table_without_set, edge_rdd, default_user)

      println(user_graph.numVertices)
      println(user_graph.numEdges)

//      println(user_graph.numVertices)

      val duration = (System.nanoTime - t1) / 1e9d
      println("The total execution time taken is " + duration.toString() + " sec.")
    }
//  Represent the graph
    //  For Python, you can use GraphFrame, you can learn more about GraphFrame from the link:
    //    https://graphframes.github.io/user-guide.html
    //  For Scala, you can use GraphFrame or GraphX, you can learn more about GraphX from the link:
    //    http://spark.apache.org/docs/latest/graphx-programming-guide.html
//  Betweenness and Modularity
    //  You are required to implement betweenness and modularity according to the lecture by yourself.
    //  The betweenness function should be calculated on the original graph. You can write a
    //    betweenness function and a modularity function, and call two functions several times until
    //  finding the max modularity value.
//    Output Format
    //  - For firstname_lastname_communities.txt
    //  Each list is a community, in which contains userIds. In each list, the userIds should be in
    //  ascending order. And all lists should be ordered by the first userId in each list in ascending order.
    //  An example is as follows: (the example just shows the format, is not a solution)
}
