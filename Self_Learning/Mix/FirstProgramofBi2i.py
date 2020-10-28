import sys


def printaname(name=""):
    if len(sys.argv) > 1:
        if name == "":
            print("Please provide a proper name")
        else:
            print("Your Name is "+name)


printaname(sys.argv[1])
