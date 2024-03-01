from pyspark import SparkContext
from pyspark.sql import SparkSession

def main():
    # spark = SparkSession.builder.appName("query1").getOrCreate()
    sc = SparkContext(master="local[*]", appName="query1-1")
    base = "hdfs:///user/cs4433/project3/"
    # test = spark.read.csv("../input/test.csv",inferSchema=False).\
    #     rdd.map(lambda y: testMap(y))
    infected_small = sc.textFile("hdfs:///user/cs4433/project3/input/INFECTED-small.csv") \
        .flatMap(lambda line: map_to_cells(line)).map(flatMapResolver)

    people_large = sc.textFile("hdfs:///user/cs4433/project3/input/PEOPLE-large.csv", 100) \
        .flatMap(lambda line: map_to_cells(line)).map(flatMapResolver)

    # infected_small.saveAsTextFile("hdfs:///user/cs4433/project3/output/infected_small")
    # people_large.saveAsTextFile("hdfs:///user/cs4433/project3/output/people_large")
    results = infected_small.join(people_large).filter(filterPairs).map(lambda x: (x[1][1][0], x[1][1][1]))
    results.saveAsTextFile("hdfs:///user/cs4433/project3/output/query1Results")
    # results2 = infected_small.map(testMap)
    results2 = results.map(lambda x: x[0]).distinct()
    results2.saveAsTextFile("hdfs:///user/cs4433/project3/output/query2Results")


def filterPairs(x):
    valInf = x[1][0]
    valPeople = x[1][1]
    if (x[1][1][0] == x[1][0][0]): return False
    if (x[1][1][3] == False): return False
    return is_close_contact([valPeople[1], valPeople[2]], [valInf[1], valInf[2]])


def flatMapResolver(x):
    return x[0], [x[1], x[2], x[3], x[4]]


def is_close_contact(attendee, infected_pid):
    distance = ((attendee[0] - infected_pid[0]) ** 2 + (attendee[1] - infected_pid[1]) ** 2) ** 0.5
    return distance <= 6


def map_to_cells(lineIn):
    output = []
    pid, x, y = [int(x) for x in lineIn.split(',')]
    # size of concert is 10,000 so we'll make cells around 25 wide to make it an even division
    base_cell = [int(x / 25), int(y / 25)]
    mod_cell = [x % 25, y % 25]
    adj_cells = [(0 if (mod_cell[0] < 19 or mod_cell[0] > 6)
                  else 1 if (mod_cell[0] >= 19) else -1),
                 (0 if (mod_cell[1] < 19 or mod_cell[1] > 6)
                  else 1 if (mod_cell[1] >= 19) else -1)]
    output.append([str(base_cell), pid, x, y, True])

    x_works = adj_cells[0] != 0 and (((adj_cells[0] + base_cell[0]) < 10000) and ((adj_cells[0] + base_cell[0]) >= 0))
    y_works = adj_cells[1] != 0 and (((adj_cells[1] + base_cell[1]) < 10000) and ((adj_cells[1] + base_cell[1]) >= 0))
    if x_works:
        output.append([str([base_cell[0] + adj_cells[0], base_cell[1]]), pid, x, y, False])
    if y_works:
        output.append([str([base_cell[0], base_cell[1] + adj_cells[1]]), pid, x, y, False])
    if x_works:
        output.append([str([base_cell[0] + adj_cells[0], base_cell[1] + adj_cells[1]]), pid, x, y, False])

    return output


if __name__ == "__main__":
    main()
