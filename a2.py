"""Connected Components using Apache Spark

FIRST_NAME: Abhyudaya
LAST_NAME: Mourya
UBIT: amourya

Basic implementation of finding connected components in large graphs using
Apache Spark (Python bindings - pySpark).

Reference: https://ai.google/research/pubs/pub43122/
"""
import sys

from pyspark import SparkContext, SparkConf


def small_star_reducer(edge):
    """Reducer for small-star operation

    The Reduce phase of the small-star operation that actually performs the
    edge re-writing of all smaller nodes than current to the smallest node
    """

    # Generate the neighborhood for a node and generate gamma+ by adding self
    current_node, neighborhood = edge
    neighborhood = list(neighborhood)
    neighborhood.append(current_node)

    # Find the smallest node(v) in neighborhood & return edges to it from all
    smallest_node = min(neighborhood)

    for node in neighborhood:
        yield node, smallest_node

def large_star_reducer(edge):
    """Reducer for large-star operation

    The Reduce phase of the large-star operation that actually performs the
    edge re-writing of all greater nodes to the smallest node
    """

    # Generate neighborhood to find the smallest neighbor & then smallest node
    current_node, neighborhood = edge
    smallest_neighbor = min(neighborhood)
    smallest_node = min(smallest_neighbor, current_node)

    # If any node in neighborhood is greater than current_node
    # write its edge to the smallest_node
    for node in neighborhood:
        if node >= current_node:
            yield node, smallest_node

def small_star_mapper(edge):
    """Mapper for small-star operation

    The Map phase of the small-star operation, that returns an edge from the
    larger of 2 nodes to the smaller of the 2 nodes
    """
    if edge[1] <= edge[0]:
        yield edge[0], edge[1]
    else:
        yield edge[1], edge[0]

def large_star_mapper(edge):
    """Mapper for large-star operation

    The Map phase of the large-star operation, that returns all possible
    combination of edges between 2 nodes so that the reducer can brute force
    its way into finding the smallest node but will only write edges from the
    larger nodes to the smallest node
    """
    yield(edge[0], edge[1])
    yield(edge[1], edge[0])

def parse_data(edge_str):
    """String to int converter

    Receive edge as in input string read by sc.textFile(), parse it by
    splitting the string into source and destination with a whitespace and
    return the integers
    """
    edge = [int(node) for node in edge_str.split(" ")]
    yield(edge[0], edge[1])
    yield(edge[1], edge[0])


if __name__ == "__main__":
    """Connected components using Map/Reduce

    Runner code that will run the actual map/reduce algorithm for finding
    connected components in large graph
    """

    # Inintialize data received as arguments
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    config = SparkConf().setAppName("ConnectedComponents").set(
            "spark.hadoop.validateOutputSpecs", "False")
    sc = SparkContext(conf=config)

    # Initialize the Spark Context
    input_str = sc.textFile(input_path)
    input_data = input_str.flatMap(parse_data)
    input_size = input_data.count()

    large_star_map = input_data.groupByKey()\
                    .flatMap(large_star_reducer)\
                    .distinct()

    small_star_map = large_star_map\
                    .flatMap(small_star_mapper)\
                    .groupByKey()\
                    .flatMap(small_star_reducer)\
                    .distinct()

    map_diff = large_star_map\
                .subtract(small_star_map)\
                .union(small_star_map\
                    .subtract(large_star_map)).count()

    while map_diff != 0:
        large_star_map = small_star_map\
                        .flatMap(large_star_mapper)\
                        .groupByKey()\
                        .flatMap(large_star_reducer)\
                        .distinct()\

        small_star_map = large_star_map\
                        .flatMap(small_star_mapper)\
                        .groupByKey()\
                        .flatMap(small_star_reducer)\
                        .distinct()

        map_diff = large_star_map\
                .subtract(small_star_map)\
                .union(small_star_map\
                    .subtract(large_star_map)).count()

    small_star_map.saveAsTextFile(output_path)

    sc.stop()
