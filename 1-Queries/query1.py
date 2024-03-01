from pyspark import SparkContext
from pyspark.sql import SparkSession




def main():
    # spark = SparkSession.builder.appName("query1").getOrCreate()
    sc = SparkContext(master="local", appName="query1")

    test = sc.textFile("../input/test.csv")
        # .map(lambda y: testMap(y))
    # infected_small =sc.textFile("../input/INFECTED-small.csv")\
    #     .flatMap(lambda line: map_to_cells(line, True)).cache()

    # people_large = sc.textFile("../input/PEOPLE-large.csv", 10) \
    #     .flatMap(lambda line: map_to_cells(line, False))

    # infected_small.saveAsTextFile("../output/test/infectedSmall")
    print(test.collect())

    sc.stop()

def testMap(yIn):
    print(yIn)
    return " "
def map_to_cells(lineIn, infected):
    print(lineIn)
    # output = []
    # pid, x, y = [int(x) for x in lineIn.split(',')]
    # # size of concert is 10,000 so we'll make cells around 25 wide to make it an even division
    # base_cell = [int(x / 25), int(y / 25)]
    # mod_cell = [x % 25, y % 25]
    # adj_cells = [(0 if (mod_cell[0] < 19 or mod_cell[0] > 6)
    #               else 1 if (mod_cell[0] >= 19) else -1),
    #              (0 if (mod_cell[1] < 19 or mod_cell[1] > 6)
    #               else 1 if (mod_cell[1] >= 19) else -1)]
    # output.append((base_cell, [pid, x, y, infected, True]))
    #
    # x_works = adj_cells[0] != 0 and (((adj_cells[0] + base_cell[0]) < 10000) and ((adj_cells[0] + base_cell[0]) >= 0))
    # y_works = adj_cells[1] != 0 and (((adj_cells[1] + base_cell[1]) < 10000) and ((adj_cells[1] + base_cell[1]) >= 0))
    # if x_works:
    #     output.append(([base_cell[0] + adj_cells[0], base_cell[1]], [pid, x, y, infected, False]))
    # if y_works:
    #     output.append(([base_cell[0], base_cell[1] + adj_cells[1]], [pid, x, y, infected, False]))
    # if x_works:
    #     output.append(([base_cell[0] + adj_cells[0], base_cell[1] + adj_cells[1]], [pid, x, y, infected, False]))

    # return output
    return ("test", "test")


if __name__ == "__main__":
    main()
