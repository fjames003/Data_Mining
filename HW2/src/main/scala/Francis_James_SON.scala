import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import sys.process._
import scala.collection.mutable

object Francis_James_SON {
    def main(args: Array[String]): Unit = {

      val sc = new SparkContext("local", "SON")

      val users_data   = sc.textFile(args(0))
      val ratings_data = sc.textFile(args(1))

      val genders = users_data.map{ line =>
        val split_line = line.split("::")
        (split_line(0).toInt, split_line(1))
      }
      val gender_broadcast = sc.broadcast(genders.collectAsMap())

//      val avg_movie_rating_by_gender = ratings_data.map{ line =>
//        val split_line = line.split("::")
//        ((split_line(1).toInt, gender_broadcast.value(split_line(0).toInt)), split_line(2).toInt)
//      }.groupByKey().mapValues(rating => rating.reduceLeft(_+_).toFloat / rating.size).sortByKey().map{
//        case ((movie, gender), rating) => List(movie, gender, rating).mkString(",")
//      }
//
//      avg_movie_rating_by_gender.collect().foreach(println(_))


      val male_user_basket = ratings_data.map{ line =>
        val split_line = line.split("::")
        ((split_line(0).toInt, gender_broadcast.value(split_line(0).toInt)), split_line(1).toInt)
      }.filter{
        case ((user, gender), movie) => gender.equals("M")
      }.groupByKey().map{
        case ((user, gender), movies) => movies
      }

      male_user_basket.repartition(10)

      println(male_user_basket.map(basket => basket.map(item => (item, 1)).reduceByKey(_+_).filter((item, support) => support >= 1300)))

//      male_user_basket.mapPartitions { partition =>
//        partition match {
//          case ((user, gender), movies) =>
//        }
//      }
//      male_user_basket.take(5).foreach(println(_))
//
    }
//
//    def apriori(baskets: Iterable[Any]) : Iterable[Any] = {
//        val freq_items_1 = Map()
//        baskets.foreach(basket => basket.foreach(item => (item, 1)).reduceByKey(_+_).filter((item, support) => support >= 1300))
//
//    }
}
