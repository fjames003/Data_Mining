import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkContext
import sys.process._

object Francis_James_task1 {
    def main(args: Array[String]): Unit = {
      val t1 = System.nanoTime
      require(args.length >= 2, "Please provide ratings.csv and testing_small.csv as arguments 1 and 2.")
      val sc = new SparkContext("local", "task1")

    //   Get CSV Files from command line and remove the headers.
    // In the form of Rating(user, movie, rating)
      val total_data = sc.textFile(args(0)).mapPartitionsWithIndex{
            (index, iterator) => if (index == 0) iterator.drop(1) else iterator
        }.map(_.split(",") match {
                case Array(user, item, rating, _) => ((user.toInt, item.toInt), rating.toDouble)
              })

    // In the form of (user, movie)
      val testing_data_temp  = sc.textFile(args(1)).mapPartitionsWithIndex{
            (index, iterator) => if (index == 0) iterator.drop(1) else iterator
        }.map(_.split(",") match {
                case Array(user, movie) => ((user.toInt, movie.toInt), 1)
            })
      val training_data = total_data.subtractByKey(testing_data_temp).map{ case ((user, movie), rating) =>
          Rating(user, movie, rating)
      }
      val testing_data = testing_data_temp.map{ case ((user, movie), _) => (user, movie)}

      // Can be used for imputation later
      val avg_ratings = training_data.map{ case Rating(_, item, rating) => (item, (rating, 1))}
        .reduceByKey{ case ((rating1, count1), (rating2, count2)) =>
          (rating1 + rating2, count1 + count2)}
        .mapValues{ case (rating_sum, rating_count) =>
          rating_sum / rating_count.toDouble
        }
      val broadcast_average = sc.broadcast(avg_ratings.collectAsMap())

      val (rank, iterations, lambda) = (5, 10, 0.01)
      val mf_model = ALS.train(training_data, rank, iterations, lambda)

    //   Get predictions and put in the form of k,v pairs
      val predictions = mf_model.predict(testing_data).map{ case Rating(user, movie, rating) =>
          ((user, movie), rating)
      }
      val no_predictions = testing_data_temp.subtractByKey(predictions)
      val guess_predictions = no_predictions.map{ case ((user, movie), _) =>
        ((user, movie), broadcast_average.value.getOrElse(movie, 2.5))
      }
      val all_predictions = predictions.union(guess_predictions)
    //   This will create k,v pairs where the key = (user, movie) and value = (predicted_rating, true_rating)
      val true_and_prediction = total_data.join(all_predictions)

      val ranges = true_and_prediction.map{ case ((_, _), (rating1, rating2)) =>
          math.abs(rating1 - rating2) match {
              case value if value >= 0 && value < 1 => (">=0 and <1", 1)
              case value if value >= 1 && value < 2 => (">=1 and <2", 1)
              case value if value >= 2 && value < 3 => (">=2 and <3", 1)
              case value if value >= 3 && value < 4 => (">=3 and <4", 1)
              case _ => (">=4", 1)
          }
      }.reduceByKey(_+_).sortByKey().map{ case (range, amount) => List(range, amount).mkString(":")}
      val n = 1 / true_and_prediction.count().toDouble
      val radical = true_and_prediction.map{ case ((_, _), (rating1, rating2)) =>
          val error = rating1 - rating2
          error * error
      }.sum()
      val rmse = math.sqrt(n * radical)
      ranges.collect().foreach(println(_))
      println("RMSE = " + rmse.toString())

      val header = sc.parallelize(Array("UserId,MovieId,Pred_rating"))
      val formatted_predictions = all_predictions.sortByKey().map(pred => List(pred._1._1, pred._1._2).mkString(",") + "," + pred._2.toString())

      header.union(formatted_predictions).coalesce(1, true).saveAsTextFile("./scala_task1_result")
      Process("mv ./scala_task1_result/part-00000 ./Francis_James_result_task1.txt") #&& Process("rm -rf ./scala_task1_result")!
      val duration = (System.nanoTime - t1) / 1e9d
      println("The total execution time taken is " + duration.toString() + " sec.")
    }
}
