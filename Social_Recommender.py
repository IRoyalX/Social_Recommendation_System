from pyspark import SparkContext, SparkConf
import os

inputPath = "input.SOCIAL.txt"

def read(connection):
    connection = connection.split("\t")
    if connection[1]:
        return (int(connection[0]), list(set(map(int, connection[1].split(",")))))
    else:
        return (int(connection[0]), [])

def make_pairs(friends):
    pairs = []
    for x in range(len(friends)):
        for y in range(x + 1, len(friends)):
            if friends[x] < friends[y]:
                pairs.append( ((friends[x], friends[y]), 1) ) 
            else:
                pairs.append( ((friends[y], friends[x]), 1) )

    return pairs

if __name__ == "__main__":
    spark = SparkContext(conf= SparkConf())
    file = spark.textFile(inputPath)
    os.system("cls")
    
    friends_list = file.map(read)                                                                   # (<user>, [<friends>, ...])
    pairs = friends_list.flatMap(lambda x: make_pairs(x[1]))                                        # ((pairs), 1)
    counts = pairs.reduceByKey(lambda x, y: x + y)                                                  # ((pairs), <count>)
    counts = counts.flatMap(lambda x: ((x[0][0], (x[0][1], x[1])), (x[0][1], (x[0][0], x[1]))) )    # [(<pair1>, (<pair2>, <count>)), (<pair2>, (<pair1>, <count>))]
                                                                                                    # (<user>, ([<friends>, ...], (<mutual>, <count>)))
    pairs = (friends_list.join(counts.groupByKey())).flatMap(lambda  x: [ (x[0], (x[1][0], y)) for y in x[1][1] ] ).groupByKey()
                                                                                                    # [<user>, [<recommendations>, ...]]                    
    result = pairs.map(lambda x: (x[0], [y[1][0] for y in sorted(x[1], key=lambda y: (-y[1][1], y[1][0])) if y[1][0] not in y[0]][:10])).sortByKey()                                              
    result = "\n".join(result.map(lambda x: f"{x[0]}\t{x[1]}").collect())

    spark.stop()

    output = inputPath.split(".")[1]
    with open(f"output.{output}.txt", "w") as file:
        file.write(result)
    
    os.system("cls")
    print(f"\nTop10 most frequent mutual friends (<user>\\t<recommendations>):\n\n{result}\n")    