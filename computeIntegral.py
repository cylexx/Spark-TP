from __future__ import print_function

import sys
import numpy as np
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("BRAULT_BOYERE_INTEGRALE")\
        .getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = float(sys.argv[2]) if len(sys.argv) > 2 else 81*81*81

    a = 1
    b = 10

    pas = (b-a)/n
    
    def f(_):
	return ((1./_)*pas)

    count = spark.sparkContext.parallelize(np.arange(1, b, pas), partitions).map(f).reduce(add)

    print("Integrate 1/x from 1 to 10 is %f" % (count))

spark.stop()
