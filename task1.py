import json
import sys
import os
import re

from pyspark import SparkContext
from operator import add

input_file = sys.argv[1]
output_file = sys.argv[2]
stopwords_file = sys.argv[3]
year_given = sys.argv[4]
m_users = int(sys.argv[5])
n_words = int(sys.argv[6])

# spark_conf = SparkConf().setAppName("task1").setMaster("local[*]").set('spark.driver.memory', '12G')
# Load review file in RDD
sc = SparkContext(appName="task1")
sc.setLogLevel("ERROR")
input_RDD = sc.textFile(input_file)
rdd_review_dict = input_RDD.map(lambda x: json.loads(x))
# rdd_review_dict = sc.textFile(input_file).map(lambda x: json.loads(x))
# Storage RDD: persist() / cache(): using persist()
# rdd_review_dict.persist()


def compute_result():

    output_dict = {'A': 0, 'B': 0, 'C': 0, 'D': [], 'E': []}
    # A. The total number of reviews (0.5pts)
    count_review = rdd_review_dict.map(lambda x: (x["review_id"], 1)).count()
    output_dict['A'] = int(count_review)

    # B. The number of reviews in a given year, y (1pts)
    # map_b = dataset.map(mapper_b).filter(lambda x: x[0].startswith(str(y))).map(lambda x: (y, x[1]))
    # # print(map_b.collect())
    # count_year = map_b.reduceByKey(add).collect()
    given_year_rdd_list = rdd_review_dict.map(lambda x: (x["date"], 1)).filter(
        lambda x: x[0][0:4] == str(year_given)).count()
    # given_year_rdd_list = rdd_review_dict.filter(lambda x: x["date"][0:4] == year_given).collect()
    output_dict['B'] = int(given_year_rdd_list)

    # C. The number of distinct users who have written the reviews (1pts)
    users = rdd_review_dict.map(lambda x: (x["user_id"], 1)).reduceByKey(add)  # Remain count for D - [user_id, count]
    output_dict['C'] = int(users.count())

    # D. Top m users who have the largest number of reviews and its count (1pts)
    # top_m_users = users.takeOrdered(int(m_users), key=lambda x: (-x[1], x[0]))
    top_m_users = users.sortBy(lambda x: x[1], False).take(int(m_users))
    output_dict['D'] = list(top_m_users)

    # # E. Top n frequent words in the review text.
    # # Pre-process stopwords
    # # Punctuations
    punctuations_list = ['(', '[', ',', '.', '!', '?', ':', ';', ']', ')']
    # # Stopwords
    stopwords_list = []
    with open(stopwords_file, 'r') as stopwords:
        # stopwords_list = stopwords.read()
        for line in stopwords.readlines():
            stopwords_list.append(line.strip())
    #
    def exclude_punctuations(rdd_dict):
        tmp = re.sub(r"[(\[,.!?:;\])]", "", rdd_dict["text"])
        return tmp, 1
    #

    def pre_process_word(word):
        tmp = ''
        if word not in stopwords_list:
            for c in word:
                if c not in punctuations_list:
                    tmp += c
        return tmp

    # read stopwords file
    file = open(stopwords_file, 'r')
    stopwords = file.read()
    # print(stopwords)

    words_dict = rdd_review_dict.map(lambda x: (x['review_id'], x['text'])).map(lambda x: x[1]).flatMap(lambda x: x.lower().split(' ')).map(lambda x: (pre_process_word(x), 1)).filter(lambda x: (x[0] is not None) and (x[0] is not "")).reduceByKey(add).takeOrdered(int(n_words), key=lambda x: (-x[1], x[0]))
    output_dict['E'] = list(map(lambda x: x[0], words_dict))

    return output_dict


# all_words = rdd_review_dict.map(exclude_punctuations).map(lambda x: x[0].lower()).flatMap(lambda x: x.split(' ')).filter(lambda x: x not in stopwords_list).map(lambda x: (x, 1))
# top_n_words = all_words.reduceByKey(add).sortBy(lambda x: x[1], False).map(lambda x: x[0]).take(int(n_words))
# output_dict['E'] = top_n_words

# print("******output_dict******* is:", output_dict)
if __name__ == "__main__":
    result = compute_result()
    # result = {'A': 0, 'B': 0, 'C': 0, 'D': [], 'E': []}
    with open(output_file, 'a') as f:
        json.dump(result, f)
