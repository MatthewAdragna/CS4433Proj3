import argparse
import random
import string

MBLINES = (int)(100000 / 1.7)


# Assuming that each line is approximately 10 bytes long
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--customers",
        help="Amount of customers generated",
        type=int,
        default=50000,
    )
    parser.add_argument(
        "--purchases",
        help="Amount of purchases created",
        type=int,
        default=5000000,
    )
    parser.add_argument(
        "--path",
        help="Path where the files will be created",
        type=str,
        default="../input/",
    )
    args = parser.parse_args()
    with open(args.path + "customers.csv", "w") as custFile:
        for i in range(0, args.customers):
            custFile.write(generateCustomer(i))

    with open(args.path + "purchases.csv", "w") as purchFile:
        for i in range(0, args.purchases):
            purchFile.write(generatePurchase(i,args.customers))


def generateCustomer(id):
    return "{},{},{},{},{}\n".format(
        id,
        "".join(random.choices(string.ascii_lowercase, k=5)),
        random.randint(18, 100),
        random.randint(1, 500),
        random.uniform(100, 10000000),
    )


def generatePurchase(id,max):
    return "{},{},{},{},{}\n".format(
        id,
        random.randint(0, max),
        random.uniform(10, 2000),
        random.randint(1, 15),
        "".join(random.choices(string.ascii_lowercase, k=random.randint(20, 50))),
    )


if __name__ == "__main__":
    main()

