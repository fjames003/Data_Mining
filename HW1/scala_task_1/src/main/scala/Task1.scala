import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import sys.process._

object Task1 {
    def main(args: Array[String]) {
        require(args.size >= 2, "Please provide users.dat and ratings.dat as arguments")
        val sc = new SparkContext("local", "task1")

        val ratings_data = sc.textFile(args(1))
        val users_data   = sc.textFile(args(0))

        val genders = users_data.map{ line =>
            val split_line = line.split("::")
            (split_line(0).toInt, split_line(1))
        }
        val genders_map = genders.collectAsMap()

        val avg_movie_rating_by_gender = ratings_data.map{ line =>
            val split_line = line.split("::")
            ((split_line(1).toInt, genders_map(split_line(0).toInt)), split_line(2).toInt)
        }.groupByKey().mapValues(rating => rating.reduceLeft(_+_).toFloat / rating.size).sortByKey().map{
            case ((movie, gender), rating) => List(movie, gender, rating).mkString(",")
        }
        avg_movie_rating_by_gender.coalesce(1, true).saveAsTextFile("./scala_task1_result")
        Process("mv ./scala_task1_result/part-00000 ./Francis_James_result_task1.txt") #&& Process("rm -rf ./scala_task1_result")!
    }
}
