from pyspark import SparkContext
from pyspark.sql import SparkSession


def main():
    sc = SparkContext(master="local[*]", appName="query3")
    people_some_infected = sc.textFile("hdfs:///user/cs4433/project3/input/PEOPLE-SOME-INFECTED-large.csv") \
        .flatMap(lambda line: map_to_cells(line)).map(flat_map_resolver)

    result3 = people_some_infected.groupByKey().mapValues(list).flatMap(flat_map_pairs)
    result3.saveAsTextFile("hdfs:///user/cs4433/project3/output/result3")
    sc.stop()


def flat_map_pairs(x):
    people = x[1]
    pairs = []
    for person in people:
        if person[4] == True:
            sumInfected = 0
            for otherPerson in people:
                if otherPerson[0] != person[0] and is_close_contact([person[1], person[2]],
                                                                    [otherPerson[1], otherPerson[2]]):
                    sumInfected += 1
            pairs.append((person[0], sumInfected))

    return pairs


def is_close_contact(attendee, infected_pid):
    distance = ((attendee[0] - infected_pid[0]) ** 2 + (attendee[1] - infected_pid[1]) ** 2) ** 0.5
    return distance <= 6


def map_to_cells(lineIn):
    output = []
    split = lineIn.split(',')
    pid, x, y = int(split[0]), int(split[1]), int(split[2])
    infectedStatus = False if split[3] == "no" else True
    # size of concert is 10,000 so we'll make cells around 25 wide to make it an even division
    base_cell = [int(x / 25), int(y / 25)]
    mod_cell = [x % 25, y % 25]
    adj_cells = [(0 if (mod_cell[0] < 19 or mod_cell[0] > 6)
                  else 1 if (mod_cell[0] >= 19) else -1),
                 (0 if (mod_cell[1] < 19 or mod_cell[1] > 6)
                  else 1 if (mod_cell[1] >= 19) else -1)]
    output.append([str(base_cell), pid, x, y, True, infectedStatus])

    x_works = adj_cells[0] != 0 and (((adj_cells[0] + base_cell[0]) < 10000) and ((adj_cells[0] + base_cell[0]) >= 0))
    y_works = adj_cells[1] != 0 and (((adj_cells[1] + base_cell[1]) < 10000) and ((adj_cells[1] + base_cell[1]) >= 0))
    if x_works:
        output.append([str([base_cell[0] + adj_cells[0], base_cell[1]]), pid, x, y, False, infectedStatus])
    if y_works:
        output.append([str([base_cell[0], base_cell[1] + adj_cells[1]]), pid, x, y, False, infectedStatus])
    if x_works:
        output.append(
            [str([base_cell[0] + adj_cells[0], base_cell[1] + adj_cells[1]]), pid, x, y, False, infectedStatus])

    return output


def flat_map_resolver(x):
    return x[0], [x[1], x[2], x[3], x[4], x[5]]


if __name__ == "__main__":
    main()
