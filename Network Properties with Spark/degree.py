import sys
import pandas
import networkx as nx
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from graphframes import *

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)


def simple(g):
    flipped = g.edges.select("dst","src")
    flippedjoin = g.edges.unionAll(flipped)
    p = GraphFrame(g.vertices,flippedjoin)
    flip =p.edges.filter("src!=dst")
    f = GraphFrame(g.vertices,flip)
    return f

def degreedist(g):
    count = g.inDegrees.selectExpr('id as id','inDegree as degree').groupby('degree').count()
    return count

def readFile(filename, large, sqlContext=sqlContext):
    lines = sc.textFile(filename)

    if large:
	delim=" "
		# Strip off header row.
	lines = lines.mapPartitionsWithIndex(lambda ind,it: iter(list(it)[1:]) if ind==0 else it)
    else:
	delim=","
    vals = lines.map(lambda x : x.split(delim))
    edges= sqlContext.createDataFrame(vals,['src' , 'dst'])
    vertices = edges.selectExpr('src as id').unionAll(edges.selectExpr('dst as id')).distinct()
    # Create graphframe g from the vertices and edges.
    a = GraphFrame(vertices,edges)
    return a


# main stuff

# If you got a file, yo, I'll parse it.
if len(sys.argv) > 1:
    filename = sys.argv[1]
    if len(sys.argv) > 2 and sys.argv[2]=='large':
	large=True
    else:
	large=False

    print("Processing input file " + filename)
    g = readFile(filename, large)

    print("Original graph has " + str(g.edges.count()) + " directed edges and " + str(g.vertices.count()) + " vertices.")

    g2 = simple(g)
    print("Simple graph has " + str(g2.edges.count()/2) + " undirected edges.")

    distrib = degreedist(g2)
    distrib.show()
    nodecount = g2.vertices.count()
    print("Graph has " + str(nodecount) + " vertices.")

    out = filename.split("/")[-1]
    print("Writing distribution to file " + out + ".csv")
    distrib.toPandas().to_csv(out + ".csv")

# Otherwise, generate some random graphs.
else:
    print("Generating random graphs.")
    vschema = StructType([StructField("id", IntegerType())])
    eschema = StructType([StructField("src", IntegerType()),StructField("dst", IntegerType())])

    gnp1 = nx.gnp_random_graph(100, 0.05, seed=1234)
    gnp2 = nx.gnp_random_graph(2000, 0.01, seed=5130303)
    gnm1 = nx.gnm_random_graph(100,1000, seed=27695)
    gnm2 = nx.gnm_random_graph(1000,100000, seed=9999)

    todo = {"gnp1": gnp1, "gnp2": gnp2, "gnm1": gnm1, "gnm2": gnm2}
    for gx in todo:
	print("Processing graph " + gx)
	v = sqlContext.createDataFrame(sc.parallelize(todo[gx].nodes()), vschema)
	e = sqlContext.createDataFrame(sc.parallelize(todo[gx].edges()), eschema)
	g = simple(GraphFrame(v,e))
	distrib = degreedist(g)
	print("Writing distribution to file " + gx + ".csv")
	distrib.toPandas().to_csv(gx + ".csv")
