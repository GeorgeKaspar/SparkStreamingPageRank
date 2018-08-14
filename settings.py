import os

APP_NAME = 'KASPAR_PAGERANK_HITS'
MASTER = 'local'
BASE_PATH = 'hdfs://data/hw4'
GRAPH_PATH = os.path.join(BASE_PATH, 'soc-LiveJournall.txt.gz')
DEBUG = False
NUM_TASKS = 10
GAMMA = 0.85
PAGERANK_NUM_ITER = 4
ALGORITHM = 'PAGERANK'  # 'HITS'
