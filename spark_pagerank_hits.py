from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import re
import sys
import os
from base64 import b64decode
import math
from operator import add

from settings import *


def createContext():
    if DEBUG:
        sc = SparkContext(MASTER, APP_NAME)
        print(sc.applicationId)
        return sc
    else:
        print(sc.applicationId)
        global sc
        return sc


def get_edges(sc, method='to', return_N=True):
    def skip_comment_and_split_to(row):
        if row.startswith('#'):
            return []
        row = row.split('\t')
        return [int(row[0]), int(row[1])]

    def skip_comment_and_split_from(row):
        if row.startswith('#'):
            return []
        row = row.split('\t')
        return [int(row[1]), int(row[0])]

    skip_comment_and_split = skip_comment_and_split_to if method == 'to' else skip_comment_and_split_from
    edges = sc.textFile(GRAPH_PATH).flatMap(skip_comment_and_split).distinct(NUM_TASKS).groupByKey(NUM_TASkS).cache()
    if not return_N:
        return edges
    N = edges.count()
    return edges, N


def init_ranks(edges, N):
    initial_value = 1.0 / N if ALGORITHM == 'PAGERANK' else 1.0 / sqrt(N) 
    ranks = edges.map(lambda x: (x[0], 1.0 / N))
    return ranks


def step_ranks_pagerank(edges, ranks, N):
    def compute_interactions(neighbors, rank):
        n = len(neighbors)
        for vertex in neighbors:
            yield vertex, rank / N

    ranks = edges.join(ranks).flatMap(lambda x: compute_interactions(x[1][0], x[1][1])).reduceByKey(add)
    # weak vertices
    ranks = ranks.mapValues(lambda value: value * gamma + (1. - gamma) / N)
    return edges, ranks, N


def pagerank(sc):
    edges, N = get_edges(sc, method='to')
    ranks = init_ranks(edges, N)
    for _ in PAGERANK_NUM_ITER:
        edges, ranks, N = step_ranks_pagerank(edges, ranks, N)

    ranks_sorted = ranks.sortBy(lambda x: x[1], ascending=False)
    for vertex, rank in ranks_sorted.take(10):
        print(vertex, rank)


def hits(sc):
    edges_to, N = get_edges(sc, method='to')
    edges_from = get_edges(sc, method='from')

    rank_to = init_ranks(edges_to, N)
    rank_from = init_ranks(edges_from, N)
    

