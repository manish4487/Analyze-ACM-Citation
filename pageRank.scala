import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.util.MurmurHash
import org.apache.spark.rdd._

object pageRank{
  def main(args: Array[String]): Unit = {
    if (args.length <2){
        System.err.println("Usage: rank <input file>  <output file>")
        System.exit(1);
      }
    
    //creating a spark session
		val spark = SparkSession.builder().appName("acm")
				.config("spark.sql.warehoue.dir", "/home/metastore_db/")
				.enableHiveSupport().master("local").getOrCreate()
				
		val sc=spark.sparkContext;
		import spark.implicits._;
		
		val input = sc.textFile(args(0)).map(i=>i.split("\\s+"))
		val pairInput = input.map(x=>(x(0),x(1)))

    //zip every vertex with an index  
    val zipPair = pairInput.map(_._1).distinct.zipWithIndex.collect.toMap
    val pairInputHash = pairInput.map{case(v1,v2)=>(zipPair.get(v1),zipPair.get(v2))}
    val cleanInput = pairInputHash.map{case(Some(v1),Some(v2))=>(v1,v2)}
		
    val edges: RDD[(VertexId,VertexId)] = cleanInput
    val graph = Graph.fromEdgeTuples(edges,null)
    
    //inlinks and outlinks for each vertex
    val inlinks = graph.inDegrees
    val outlinks = graph.outDegrees
    
    //combining them in a single rdd
    val joinInOutLinks = inlinks.join(outlinks)
    
    //storing them inlinks and outlinks into the map
    val inlinksMap = inlinks.collect().toMap
    val outlinksMap = outlinks.collect().toMap
    
    //finding all the outlinks and inlinks of paper2
    val paper2 = edges.map{case(p1,p2)=>p2}
    val paper2InOut = paper2.map{case(p2)=>(p2,outlinksMap.get(p2),inlinksMap.get(p2))}
    val p2Inout = paper2InOut.map{case(p2,Some(out),Some(in))=>(p2,(out,in))}
    
    //combining the result with with edges
    val edgesinout = edges.zip(p2Inout)
    
    val cleanInOut = edgesinout.map{case((p1,p2),(duplicate_p2,(out,in)))=>(p1,p2,out,in)}
    
    //adding up all the inlinks and outlinks for a vertex to calculate its Win and Wout
    
    
    //result.rdd.saveAsTextFile(args(1))
    sc.stop()
  }  
}