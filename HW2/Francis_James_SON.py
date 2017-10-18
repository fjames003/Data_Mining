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
    freq_items[1] = set(singleton for singleton in item_counts if item_counts[singleton] >= support)

    k = 2
    basket_groups = []
    while freq_items[k - 1]:
        print("The Value of K is: {0}".format(k))

        # Generate candidates
        print("\tCreating Candidate Items")
        if k == 2:
            candidate_items = set(comb for comb in combinations(freq_items[1], 2))
        # Must use monotonicity to avoid creating unecessary pairs
        else:
            # First find the distinct items from pairs, and use to make triples
            candidate_items = set(value for tup in freq_items[k - 1] for value in tup)
            # Now make k combinations with distinct items
            candidate_items = set(comb for comb in combinations(candidate_items, k))
        if not candidate_items:
            break
        else:
            print("\tRemoving non-frequent items from baskets")
            # Remove non frequent singletons to make computation easier
            new_baskets = []
            # Should find a way to use RDD and filter...
            # May need to broadcast freq_items
            # rdd.foreach(lambda basket: basket.filter(lambda movie: movie in freq_items[1]))
            for basket in baskets:
                new_basket = filter(lambda movie: movie in freq_items[1], basket)
                new_baskets.append(new_basket)
            baskets = new_baskets

            print("\tGenerating combinations of basket items and counting") # By far the slowest step...
            # Generate the pairs found in a given basket in order to count
            item_counts = {}
            for basket in baskets:
                # Combinations are unique and thus I know the count of each tuple is 1...
                basket_group = set(comb for comb in combinations(basket, k))

                # Can also filter basket_group for tuples in candidate_items and then run a map to count all tuples left in basket group
                # Just need basket group to be a RDD
                candidate_basket = basket_group.intersection(candidate_items)
                for tup in candidate_basket:
                    item_counts[tup] = item_counts.get(tup, 0) + 1

            print("\tPruning")
            # Now that we have all the counts, prune with support
            freq_items[k] = set(key for key,value in item_counts.items() if value >= support)

            # Increment the tuple size
            k += 1
    # Dictionary where key is the tuple size and value is set of items with enough support
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

    genders = users_data.map(lambda line: line.split("::")).map(lambda line: (int(line[0]), str(line[1])))
    # Map that looks like {User 1: Gender 1, User 2: Gender 2...}
    genders_map = genders.collectAsMap()

    if case_number == 1:
        # Run SON to find FREQUENT MOVIES RATED BY MALE USERS
        # ---------------------------------------------------
        # Map 1:
            # Input is a chunk/subset of all baskets; fraction p of total input file
            # Find itemsets frequent in that subset (e.g., using apriori)
            # Use support threshold ps
            # Output is set of key-value pairs (F, 1) where F is a frequent itemset from sample

        # Generate baskets from rating file consistent with assignment specs...
        #  More specifically, the movie ids are unique within each basket. aka use a set...

        male_user_baskets = ratings_data.map(lambda line: line.split("::")) \
                             .map(lambda line: ((int(line[0]), genders_map[int(line[0])]), int(line[1]))) \
                             .filter(lambda ((user, gender), movie): gender is 'M') \
                             .groupByKey()

        # Set up chunks for SON.
        numPartitions = 4
        support_adjustment_p = 1 / float(numPartitions)

        # Create a known number of chunks to break up support by.
        male_user_baskets.repartition(numPartitions)

        # This is my first problem, I am passing apriori a list, when I want to pass an RDD...
        # My second problem is that groupByKey() returns 'ResultIterable'...
        def partitioned_apriori(iterator):
            basket_of_ratings = []
            for ((user, gender), movies) in iterator:
                basket_of_ratings.append(movies)
            freq_subset = apriori(basket_of_ratings, support_thr * support_adjustment_p)
            return [(movie, 1) for tuple_size in freq_subset for movie in freq_subset[tuple_size]]

        # This should produce (k,v) pairs like (Frequent_Item, 1)
        freq_movies = male_user_baskets.mapPartitions(partitioned_apriori)
        print(freq_movies.take(5))

        # Reduce 1: (I think this reduce task is just for the benefit of the grouping operation, I could just explicitly group)
            # Each reduce task is assigned set of keys, which are itemsets
            # Produces keys that appear one or more time
            # Frequent in some subset
            # These are candidate itemsets

        # freq_movies = freq_movies.groupByKey()

        # Map 2:
        #     Each Map task takes output from first Reduce task AND a chunk of the total input data file
        #     All candidate itemsets go to every Map task
            #  Where candidate itemsets are the frequent items found with apriori, thus broadcast frequent items...
        #     Count occurrences of each candidate itemset among the baskets in the input chunk
        #     Output is set of key-value pairs (C, v), where C is a candidate frequent itemset and v is the support for that
        #         itemset among the baskets in the input chunk

        # Reduce 2:
        #   Take output from second map task and reduce by key adding the support then filter based on support and output

    elif case_number == 2:
        # Run SON to find FREQUENT FEMALE USERS WHO RATED MOVIES
        pass
    else:
        print("Case number must be either 1 or 2.")
        exit(1)
