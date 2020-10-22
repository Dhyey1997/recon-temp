import pyspark
from pyspark import SparkContext

# LOADING THE DATASET
def getWashevents(sc, washevents_path):
    washevents = sc.read.csv(washevents_path, header=True)
    return washevents
    
# WRITING THE OUTPUT
def writeToCSV(df, path):
    df.toPandas().to_csv(path)
