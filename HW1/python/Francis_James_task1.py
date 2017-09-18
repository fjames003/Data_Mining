import sys
from pyspark import SparkContext
from os import system

if __name__ == '__main__':

    # Make new spark context
    sc = SparkContext("local", "task1")

    # Make sure user passed the to required files as arguments, if not print usage
    if len(sys.argv[1:]) < 2:
        print("Usage: $SPARK_HOME/spark-submit Francis_James_result_task1.py users.dat ratings.dat")
        exit(1)

    # Load arguments into variables after reading text files.
    ratings_data = sc.textFile(sys.argv[2])
    users_data   = sc.textFile(sys.argv[1])

    genders = users_data.map(lambda line: line.split("::")).map(lambda line: (int(line[0]), str(line[1])))
    genders_map = genders.collectAsMap()
    # Break line up into parts
    # Create (K,V) pairs where the key is (MovieID, USER.gender) and value is rating
    # Group by key so that all ratings by same gender are together for each movie
    # Calculate the average rating of each movie based on gender
    # Sort the result to write to file
    # Reformat data so they are lists that way we can simply use join to produce a csv type output
    avg_movie_rating_by_gender = ratings_data \
                         .map(lambda line: line.split("::")) \
                         .map(lambda rating: ((int(rating[1]), genders_map[int(rating[0])]), int(rating[2]))) \
                         .groupByKey() \
                         .mapValues(lambda rating: sum(rating) / float(len(rating))) \
                         .sortByKey() \
                         .map(lambda result: ",".join([str(e) for e in list(result[0][:]) + list(result[1:])]))
    # Now write to a file, using coalesce to gauruntee one file, although it should always be one file.
    avg_movie_rating_by_gender.coalesce(1, True).saveAsTextFile("./task1_result")

    # In order to change name of output to correct format and eliminate useless folder
    system("mv ./task1_result/part-00000 ./Francis_James_result_task1.txt")
    system("rm -rf ./task1_result")
