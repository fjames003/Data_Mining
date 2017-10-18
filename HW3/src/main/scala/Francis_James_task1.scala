import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import sys.process._

object Francis_James_task1 {
    def main(args: Array[String]): Unit = {
      require(args.size >= 2, "Please provide ratings.csv and testing_small.csv as arguments 1 and 2.")

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
      val testing_data = testing_data_temp.map{ case ((user, movie), nothing) => (user, movie)}

      val (rank, iterations, lambda) = (5, 10, 0.01)

      val mf_model = ALS.train(training_data, rank, iterations, lambda)

    //   Get predictions and put in the form of k,v pairs
      val predictions = mf_model.predict(testing_data).map{ case Rating(user, movie, rating) =>
          ((user, movie), rating)
      }

    //   This will create k,v pairs where the key = (user, movie) and value = (predicted_rating, true_rating)
      val true_and_prediction = total_data.join(predictions)

      val ranges = true_and_prediction.map{ case ((user, movie), (rating1, rating2)) =>
          math.abs(rating1 - rating2) match {
              case value if (value >= 0 && value < 1) => (">=0 and <1", 1)
              case value if (value >= 1 && value < 2) => (">=1 and <2", 1)
              case value if (value >= 2 && value < 3) => (">=2 and <3", 1)
              case value if (value >= 3 && value < 4) => (">=3 and <4", 1)
              case _ => (">=4:", 1)
          }
      }.reduceByKey(_+_).sortByKey().map{ case (range, amount) => List(range, amount).mkString(":")}
      val n = (1 / true_and_prediction.count().toDouble)
      val radical = true_and_prediction.map{ case ((user, movie), (rating1, rating2)) =>
          val error = (rating1 - rating2)
          error * error
      }.sum()
      val rmse = math.sqrt(n * radical)
      ranges.collect().foreach(println(_))
      println(rmse)

    }
}
