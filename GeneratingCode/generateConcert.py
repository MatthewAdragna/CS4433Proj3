import argparse
import random

MBLINES = (int)(100000 / 1.7)
FILEPATHS = ["PEOPLE-large.csv", "INFECTED-small.csv", "PEOPLE-SOME-INFECTED-large.csv"]


# Assuming that each line is approximately 10 bytes long
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--size",
        help="Approximate size of the generated dataset",
        type=float,
        default=10,
    )
    parser.add_argument(
        "--path",
        help="Path where the files will be created",
        type=str,
        default="../input/",
    )
    parser.add_argument(
        "--covidchance",
        help="Pvalue between 0 and 1 that a person has covid",
        type=float,
        default=0.01,
    )
    args = parser.parse_args()

    with open(args.path + FILEPATHS[0], "w") as plF:
        with open(args.path + FILEPATHS[1], "w") as isF:
            with open(args.path + FILEPATHS[2], "w") as psiF:
                for i in range(0, MBLINES * args.size):
                    hascovid = random.random() <= args.covidchance
                    coordinates = generatecoordinates(i)

                    plF.write(coordinates + "\n")
                    if hascovid:
                        isF.write(coordinates + "\n")

                    psiF.write(coordinates + (",yes" if hascovid else ",no") + "\n")


def generatecoordinates(id):
    return "{},{},{}".format(id, random.randint(1, 10000), random.randint(1, 10000))


if __name__ == "__main__":
    main()

