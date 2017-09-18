import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import sys.process._

object Task2 {
    def main(args: Array[String]) {
        require(args.size >= 3, "Please provide users.dat ratings.dat and movies.dat as arguments")
        val sc = new SparkContext("local", "task2")

        val ratings_data = sc.textFile(args(1))
        val users_data   = sc.textFile(args(0))
        val movies_data  = sc.textFile(args(2))

        val genders = users_data.map{ (line: String) =>
            val split_line = line.split("::")
            (split_line(0).toInt, split_line(1))
        }
        val genders_map = genders.collectAsMap()

        val genres = movies_data.map{ (line: String) =>
            val split_line = line.split("::")
            (split_line(0).toInt, split_line(2))
        }
        val genres_map = genres.collectAsMap()

        val avg_genres_rating_by_gender = ratings_data.map{ (line: String) =>
            val split_line = line.split("::")
            ((genres_map(split_line(1).toInt), genders_map(split_line(0).toInt)), split_line(2).toInt)
        }.groupByKey().mapValues((rating: Iterable[Int]) => rating.reduceLeft(_+_).toFloat / rating.size).sortByKey().map{
            case ((genres, gender), rating) => List(genres, gender, rating).mkString(",")
        }
        avg_genres_rating_by_gender.coalesce(1, true).saveAsTextFile("./scala_task2_result")
        Process("mv ./scala_task2_result/part-00000 ./Francis_James_result_task2.txt") #&& Process("rm -rf ./scala_task2_result")!
    }
}
