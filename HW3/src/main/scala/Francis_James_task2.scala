import org.apache.spark.SparkContext
import scala.collection.immutable.SortedSet
import sys.process._


object Francis_James_task2 {
  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
    require(args.length >= 2, "Please provide ratings.csv and testing_small.csv as arguments 1 and 2 respectively.")
    val sc = new SparkContext("local", "task2")
    val neighborhood = 5

    // All rating data, will need to subtract testing data
    val total_data = sc.textFile(args(0)).mapPartitionsWithIndex{
      (index, iterator) => if (index == 0) iterator.drop(1) else iterator
    }.map(_.split(",") match {
      case Array(user, item, rating, _) => ((user.toInt, item.toInt), rating.toDouble)
    })

    // The keys of testing_data_temp will match total data so we can subtract by key.
    val testing_data_with_rating  = sc.textFile(args(1)).mapPartitionsWithIndex{
      (index, iterator) => if (index == 0) iterator.drop(1) else iterator
    }.map(_.split(",") match {
      case Array(user, movie) => ((user.toInt, movie.toInt), 1)
    })


    val training_data = total_data.subtractByKey(testing_data_with_rating)
    // Can get rid of dummy rating used to create matching k,v pairs with total data
    val testing_data = testing_data_with_rating.map{ case ((user, movie), _) => (user, movie)}

    // Can be used for imputation later
    val avg_movie_ratings = training_data.map{ case ((_, item), rating) => (item, (rating, 1))}
      .reduceByKey{ case ((rating1, count1), (rating2, count2)) =>
        (rating1 + rating2, count1 + count2)}
      .mapValues{ case (rating_sum, rating_count) =>
        rating_sum / rating_count.toDouble
      }
    val broadcast_movie_average = sc.broadcast(avg_movie_ratings.collectAsMap())

    // Needed for prediction
    val avg_user_rating = training_data.map { case ((user, _), rating) => (user, (rating, 1))}
        .reduceByKey{ case ((rating1, count1), (rating2, count2)) =>
          (rating1 + rating2, count1 + count2)
        }.mapValues{ case (rating_sum, rating_count) => rating_sum / rating_count.toDouble}
          .join{training_data.map{ case ((user, movie), rating) => (user, (movie, rating))}}
          .map{ case (user, (avg_r, (movie, rating))) => ((user, movie), (rating, avg_r))}
    val user_average_only = avg_user_rating.map{ case ((user, _), (_, avg_r)) => (user, avg_r)}

    val broadcast_user_average_only = sc.broadcast(user_average_only.collectAsMap())
    val broadcast_user_average  = sc.broadcast(avg_user_rating.collectAsMap())

    // User-Item CF:

    val user_movie_list = training_data.map{ case ((user, movie), _) =>
      (user, Set(movie))
    }.reduceByKey{_++_}

    val broadcasted_training = sc.broadcast(training_data.collectAsMap())

    val users_only = training_data.map{ case ((user, _), _) => user}.distinct()
    val user_user_pairs = users_only.cartesian(users_only).filter{ case (user1, user2) => user1 < user2}
//
    val co_rated_movies = user_user_pairs.map{ case (user1, user2) => (user1, (user1, user2))}
                                         .join(user_movie_list)
                                         .map{ case (_,((user1, user2), user1_movie_list)) =>
                                           (user2, ((user1, user2), user1_movie_list))}
                                         .join(user_movie_list)
                                         .map{ case (_, (((user1, user2), user1_movie_list), user2_movie_list)) =>
                                           ((user1, user2), user1_movie_list.intersect(user2_movie_list))
                                         }.filter{ case ((_, _), movie_set) => movie_set.nonEmpty}

    // This will create RDD with k,v pairs:
    // key   = (user_u, user_v)
    // value = Set((movie_i, rating_u, rating_v), (movie_j, rating_u, rating_v) where movies i and j are both rated by u and v...
    val co_rated_ratings = co_rated_movies.map{ case ((user1, user2), movie_set) =>
      ((user1, user2), movie_set.map{ case (movie) =>
        (movie,(broadcasted_training.value.getOrElse((user1,movie), 2.5), broadcasted_training.value.getOrElse((user2, movie), 2.5)))
      })
    }

    val avg_corated_rating = co_rated_ratings.map{ case ((user1, user2), movie_set) =>
            ((user1, user2), movie_set.map{ case (_, (rating1, rating2)) =>
              ((rating1, 1), (rating2, 1))
            })
          }.flatMap{ case (key, values) => values.map((key, _))
          }.reduceByKey{ case (((user1_rating1, user1_count1), (user2_rating1, user2_count1)), ((user1_rating2, user1_count2), (user2_rating2, user2_count2))) =>
              ((user1_rating1 + user1_rating2, user1_count1 + user1_count2), (user2_rating1 + user2_rating2, user2_count1 + user2_count2))
          }.mapValues{ case ((user1_sum, user1_count), (user2_sum, user2_count)) => (user1_sum / user1_count.toDouble, user2_sum / user2_count.toDouble)}

    val broadcasted_avg_corating = sc.broadcast(avg_corated_rating.collectAsMap())

    val pearson_weights = co_rated_ratings.flatMap{
            case (key, values) => values.map{ case (_, (rating1, rating2)) =>
              val avg_rating = broadcasted_avg_corating.value.getOrElse(key, (0.0, 0.0))
              (key, (rating1 - avg_rating._1, rating2 - avg_rating._2, 0.0))}
          }.mapValues{ case (normal_rating1, normal_rating2, _) =>
            (normal_rating1 * normal_rating2, math.pow(normal_rating1, 2), math.pow(normal_rating2, 2))
          }.reduceByKey{ case ((num, den_a, den_b), (num2, den_a2, den_b2)) => (num + num2, den_a + den_a2, den_b + den_b2)
          }.mapValues{ case (num, den_a, den_b) =>
            val denominator = math.sqrt(den_a) * math.sqrt(den_b)
            if (denominator == 0) {
              0.0
            } else {
              num / denominator
            }
          }


    // To make prediction for user_u and movie_i now, need average rating for each user across all movies -check
    // Need list of users similar to user_u that have rated movie_i
    val valid_users_for_v = co_rated_movies.map{
        case ((user_u, user_v), _) => (user_u, (user_u, user_v))}
      .join(user_movie_list).map{
        case (_, ((user_u, user_v), u_movie_set)) => (user_v, ((user_u, user_v), u_movie_set))}
      .join(testing_data)
      .map{ case (_, (((user_u, user_v), u_movie_set), v_target_movie)) => ((user_u, user_v), (u_movie_set, v_target_movie))}
      .filter{ case ((_, _), (u_movie_set, v_target_movie)) => u_movie_set.contains(v_target_movie)}
      .map{ case ((user_u, user_v), (_, v_target_movie)) => ((user_v, user_u), v_target_movie)}


    val valid_users_for_u = co_rated_movies.map{
      case ((user_u, user_v), _) => (user_v, (user_u, user_v))}
      .join(user_movie_list).map{
      case (_, ((user_u, user_v), v_movie_set)) => (user_u, ((user_u, user_v), v_movie_set))}
      .join(testing_data)
      .map{ case (_, (((user_u, user_v), v_movie_set), u_target_movie)) => ((user_u, user_v), (v_movie_set, u_target_movie))}
      .filter{ case ((_, _), (v_movie_set, u_target_movie)) => v_movie_set.contains(u_target_movie)}
      .map{ case ((user_u, user_v), (_, u_target_movie)) => ((user_u, user_v), u_target_movie)}

    // Will produce RDD with ((user_to_predict, valid_user), (movie_to_predict, weight_for_valid_user))
    val valid_users_for_prediction = valid_users_for_u.union(valid_users_for_v).join(pearson_weights)
      .map{ case ((user_to_predict, valid_user), (movie_to_predict, weight_for_valid_user)) =>
        ((user_to_predict, movie_to_predict), SortedSet((valid_user, weight_for_valid_user)))}
      .reduceByKey(_++_)

    // RDD of ((user, movie), neighborhood_set)
    val neighborhood_users = valid_users_for_prediction.mapValues{ neighbors => neighbors.drop(neighbors.size - neighborhood)}
    val predictions = neighborhood_users.flatMap{
      case ((user_to_pred, movie_to_pred), values) =>
        values.map{
          case (valid_user, weight) =>
            val avg_r = broadcast_user_average.value.getOrElse((valid_user, movie_to_pred), (2.5, 2.5))
            ((user_to_pred, movie_to_pred), (weight, avg_r._1 - avg_r._2))}}
      .reduceByKey{ case ((weight1, normalized_rating1), (weight2, normalized_rating2)) =>
        (math.abs(weight1) + math.abs(weight2), normalized_rating1 * weight1 + normalized_rating2 * weight2)
      }.map{ case ((user, movie), (denominator, numerator)) =>
        val user_avg = broadcast_user_average_only.value.getOrElse(user, 2.5)
        if (denominator == 0.0) {
          ((user, movie), user_avg)
        } else {
          val rating = user_avg + (numerator / denominator)
          ((user, movie), rating)
        }
      }

    val new_training_data = training_data.union(predictions.filter{ case ((_,_), rating) => rating < 5.0 && rating > 0.0})

    val new_avg_movie_ratings = new_training_data.map{ case ((_, item), rating) => (item, (rating, 1))}
      .reduceByKey{ case ((rating1, count1), (rating2, count2)) =>
        (rating1 + rating2, count1 + count2)}
      .mapValues{ case (rating_sum, rating_count) =>
        rating_sum / rating_count.toDouble
      }
    val new_broadcast_movie_average = sc.broadcast(new_avg_movie_ratings.collectAsMap())

    // Needed for prediction
    val new_avg_user_rating = new_training_data.map { case ((user, _), rating) => (user, (rating, 1))}
      .reduceByKey{ case ((rating1, count1), (rating2, count2)) =>
        (rating1 + rating2, count1 + count2)
      }.mapValues{ case (rating_sum, rating_count) => rating_sum / rating_count.toDouble}
      .join{new_training_data.map{ case ((user, movie), rating) => (user, (movie, rating))}}
      .map{ case (user, (avg_r, (movie, rating))) => ((user, movie), (rating, avg_r))}
    val new_user_average_only = new_avg_user_rating.map{ case ((user, _), (_, avg_r)) => (user, avg_r)}

    val new_broadcast_user_average_only = sc.broadcast(new_user_average_only.collectAsMap())

    val new_predictions = predictions.map{ case ((user, movie), rating) =>
      if (rating > 5.0) {
        ((user, movie), new_broadcast_user_average_only.value.getOrElse(user, new_broadcast_movie_average.value.getOrElse(movie, 3.0)))
      } else {
        if (rating < 0.0) {
          ((user, movie), new_broadcast_user_average_only.value.getOrElse(user, new_broadcast_movie_average.value.getOrElse(movie, 2.0)))
        } else {
          ((user, movie), rating)
        }
      }
    }

    val no_predictions = testing_data_with_rating.subtractByKey(new_predictions)
    val guess_predictions = no_predictions.map{ case ((user, movie), _) =>
      ((user, movie), new_broadcast_user_average_only.value.getOrElse(user, new_broadcast_movie_average.value.getOrElse(movie, 2.3)))
    }
    val all_predictions = new_predictions.union(guess_predictions)

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

    header.union(formatted_predictions).coalesce(1, true).saveAsTextFile("./scala_task2_result")
    Process("mv ./scala_task2_result/part-00000 ./Francis_James_result_task2.txt") #&& Process("rm -rf ./scala_task2_result")!
    val duration = (System.nanoTime - t1) / 1e9d
    println("The total execution time taken is " + duration.toString() + " sec.")
  }
}
