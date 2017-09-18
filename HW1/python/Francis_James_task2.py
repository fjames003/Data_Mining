import sys
from pyspark import SparkContext
from os import system

if __name__ == '__main__':
    # Make new spark context
    sc = SparkContext("local", "task2")

    # Make sure user passed the to required files as arguments, if not print usage
    if len(sys.argv[1:]) < 3:
        print("Usage: $SPARK_HOME/spark-submit Francis_James_result_task2.py users.dat ratings.dat movies.dat")
        exit(1)

    users_data = sc.textFile(sys.argv[1])
    # UserID::MovieID::Rating::Timestamp
    ratings_data = sc.textFile(sys.argv[2])
    movies_data = sc.textFile(sys.argv[3])

    genders = users_data.map(lambda line: line.split("::")).map(lambda line: (int(line[0]), str(line[1])))
    genders_map = genders.collectAsMap()

    genres = movies_data.map(lambda line: line.split("::")).map(lambda movie: (int(movie[0]), str(movie[2])))
    genres_map = genres.collectAsMap()

    avg_genres_rating_by_gender = ratings_data \
                                    .map(lambda line: line.split("::")) \
                                    .map(lambda rating: ((genres_map[int(rating[1])], genders_map[int(rating[0])]), int(rating[2]))) \
                                    .groupByKey() \
                                    .mapValues(lambda rating: sum(rating) / float(len(rating))) \
                                    .sortByKey() \
                                    .map(lambda result: ",".join([str(e) for e in list(result[0][:]) + list(result[1:])]))

    avg_genres_rating_by_gender.coalesce(1, True).saveAsTextFile("./task2_result")
    
    system("mv ./task2_result/part-00000 ./Francis_James_result_task2.txt")
    system("rm -rf ./task2_result")
