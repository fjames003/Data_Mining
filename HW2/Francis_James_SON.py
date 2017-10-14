import sys
from pyspark import SparkContext
from os import system
from itertools import combinations



# Generate Freq items of size one
# Get all combinations of freq size 1 items = candidate pairs
# Prune all pairs without min support = freq pairs
# Generate all triples by getting combinations of pairs and singles = candidate triples
# Prune all triples without min support = freq size 4 tuples
# Keep going until candidate set = null or freq set = null
# Item counts of size 1

def apriori(baskets, support):
    item_counts     = {}
    freq_items      = {}
    candidate_items = []

    # Get counts of individual items in the basket
    # basket_contains_more_baskets = any(isinstance(basket, list) for basket in baskets)
    for single_basket in baskets:
        for item in single_basket:
            item_counts[item] = item_counts.get(item, 0) + 1
    # Prune items without min support
    freq_items[1] = [singleton for singleton in item_counts if item_counts[singleton] >= support]

    k = 2
    basket_groups = []
    while freq_items[k - 1]:

        # Generate candidates
        if k == 2:
            candidate_items = [comb for comb in combinations(freq_items[1], 2)]
        # Must use monotonicity to avoid creating unecessary pairs
        else:
            distinct_values = set(value for tup in freq_items[k - 1] for value in tup)
            candidate_items= [comb for comb in combinations(distinct_values, k)]
        if not candidate_items:
            break
        else:
            # Remove non frequent singletons to make computation easier
            new_baskets = []
            for basket in baskets:
                new_basket = filter(lambda movie: movie in freq_items[1], basket)
                new_baskets.append(new_basket)
            baskets = new_baskets

            # Prune
            item_counts = {}
            for basket in baskets:

                basket_group = [comb for comb in combinations(basket, k)]

                for tup in basket_group:
                    if tup in candidate_items[k]:
                        item_counts[freq_tuple] = item_counts.get(freq_tuple, 0) + basket_group.count(freq_tuple)

            freq_items[k] = [freq_tuple for freq_tuple in candidate_items if
                             item_counts.get(freq_tuple, 0) >= support]
            k += 1
    return freq_items

if __name__ == '__main__':

    # Make new spark context
    sc = SparkContext("local", "SON")

    # Make sure user passed the required files as arguments as well as desired output parameters, if not print usage
    if len(sys.argv[1:]) < 4:
        print("Usage: $SPARK_HOME/spark-submit Francis_James_result_task1.py [Case Number] users.dat ratings.dat [Support Threshold]")
        exit(1)

    # Load arguments into variables and read text files.
    case_number  = int(sys.argv[1])
    # UserID::Gender::Age::Occupation::Zip-code
    users_data   = sc.textFile(sys.argv[2])
    # UserID::MovieID::Rating::Timestamp
    ratings_data = sc.textFile(sys.argv[3])
    support_thr  = int(sys.argv[4])

    # Set up chunks for SON.
    numPartitions = 10
    support_adjustment_p = 1 / float(numPartitions)

    genders = users_data.map(lambda line: line.split("::")).map(lambda line: (int(line[0]), str(line[1])))
    # Map that looks like {User 1: Gender 1, User 2: Gender 2...}
    genders_map = genders.collectAsMap()

    def partitioned_apriori(partition):
        print("Patition number is: {}".format(len(partition)))
        basket_of_ratings = []
        for ((user, gender), movies) in partition:
            basket_of_ratings.append(movies)

        freq_subset = apriori(basket_of_ratings, support_thr * support_adjustment_p)
        return [(movie, 1) for tuple_size in freq_subset for movie in freq_subset[tuple_size]]


    if case_number == 1:
        # Run SON to find FREQUENT MOVIES RATED BY MALE USERS
        # ---------------------------------------------------

        # Map 1:
            # Input is a chunk/subset of all baskets; fraction p of total input file
            # Find itemsets frequent in that subset (e.g., using apriori)
            # Use support threshold ps
            # Output is set of key-value pairs (F, 1) where F is a frequent itemset from sample

        # Generate baskets from rating file consistent with assignment specs...
        male_user_baskets = ratings_data.map(lambda line: line.split("::")) \
                             .map(lambda line: ((int(line[0]), genders_map[int(line[0])]), int(line[1]))) \
                             .filter(lambda ((user, gender), movie): gender is 'M') \
                             .groupByKey()

        # Create a known number of chunks to break up support by.
        male_user_baskets.repartition(numPartitions)

        # This should produce (k,v) pairs like (Frequent_Item, 1)
        freq_movies = male_user_baskets.mapPartitions(partitioned_apriori)
        #                                .reduceByKey(lambda (freq_a, freq_b): dict(freq_a.items() + freq_b.items() + [(key, a[k] + b[k]) for key in freq_b.viewkeys() & freq_a.viewkeys()]))

        # Reduce 1: (I think this reduce task is just for the benefit of the grouping operation, I could just explicitly group)
            # Each reduce task is assigned set of keys, which are itemsets
            # Produces keys that appear one or more time
            # Frequent in some subset
            # These are candidate itemsets
        # freq_movies = freq_movies.groupByKey()

        print(freq_movies.collect())

        # Map 2:
        #     Each Map task takes output from first Reduce task AND a chunk of the total input data file
        #     All candidate itemsets go to every Map task
        #     Count occurrences of each candidate itemset among the baskets in the input chunk
        #     Output is set of key-value pairs (C, v), where C is a candidate frequent itemset and v is the support for that
        #         itemset among the baskets in the input chunk

    elif case_number == 2:
        # Run SON to find FREQUENT FEMALE USERS WHO RATED MOVIES
        pass
    else:
        print("Case number must be either 1 or 2.")
        exit(1)
