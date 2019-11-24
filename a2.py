"""Connected Components using Apache Spark

FIRST_NAME: Abhyudaya
LAST_NAME: Mourya
UBIT: amourya

Basic implementation of finding connected components in large graphs using
Apache Spark (Python bindings - pySpark).

Reference: https://ai.google/research/pubs/pub43122/
"""
import sys

from pyspark import SparkContext


def large_star(edge):
    """
    Actual implementation of the large star operation being performed in the
    Map/Reduce paper that runs on the node and its neighborhood
    """
    pass

def small_star(edge):
    """
    Actual implementation of the small star operation being performed in the
    Map/Reduce paper that runs on the node and its neighborhood
    """
    pass

def large_star_mapper(edge):
    """
    edge will represent one line of the input file such that edge[0] will be
    the source and edge[1] will be the destination
    """
    pass

def small_star_mapper(edge):
    """
    edge represents one line of the input file such that edge[0] will be the
    source and edge[1] will be the destination of an edge in graph
    """
    pass


if __name__ == "__main__":
    sc = SparkContext()
    E = sc.textFile(sys.argv[1])
    n = E.count()
    print("Count of objects is {}".format(n))
    sc.stop()
