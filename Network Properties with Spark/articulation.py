import sys
import time
import networkx as nx
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from copy import deepcopy

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def articulations(g, usegraphframe=False):
	
    count = g.connectedComponents().select('component').distinct().count()

    if usegraphframe:
		
        vertex = g.vertices.map(lambda x:x.id).collect()

		
	articulation = []
        for points in vertex:
            a = GraphFrame(g.vertices.filter('id!= "'+points+'"'),g.edges.filter("src != "'+points+'"").filter("dst!="'+points+'""))
            articulationcount = a.connectedComponents().select('component').distinct().count()
            articulation.append((points,1 if articulationcount>count else 0))
        return sqlContext.createDataFrame(articulation,['id','articulation'])
            

    else:
        
        gx = nx.Graph()
        gx.add_nodes_from(g.vertices.map(lambda x:x.id).collect())
        gx.add_edges_from(g.edges.map(lambda x:(x.src,x.dst)).collect())
        
        def components(n):
            g= deepcopy(gx)
            g.remove_node(n)
            return nx.number_connected_components(g)
        
        return sqlContext.createDataFrame(g.vertices.map(lambda x:(x.id,1 if components(x.id) > count else 0)),['id','articulation'])
             
             
		

filename = sys.argv[1]
lines = sc.textFile(filename)

pairs = lines.map(lambda s: s.split(","))
e = sqlContext.createDataFrame(pairs,['src','dst'])
e = e.unionAll(e.selectExpr('src as dst','dst as src')).distinct() # Ensure undirectedness 	

# Extract all endpoints from input file and make a single column frame.
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()	

# Create graphframe from the vertices and edges.
g = GraphFrame(v,e)

#Runtime approximately 5 minutes
print("---------------------------")
print("Processing graph using Spark iteration over nodes and serial (networkx) connectedness calculations")
init = time.time()
df = articulations(g, False)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
df.toPandas().to_csv("articulation_out.csv")
print("---------------------------")

#Runtime for below is more than 2 hours
print("Processing graph using serial iteration over nodes and GraphFrame connectedness calculations")
init = time.time()
df = articulations(g, True)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
