import sys

from pyspark import SparkContext


if __name__ == "__main__":
    sc = SparkContext()
    E = sc.textFile(sys.argv[1])
    n = E.count()
    print("Count of objects is {}".format(n))
    sc.stop()
