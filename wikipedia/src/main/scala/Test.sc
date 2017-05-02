import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import wikipedia.{WikipediaData, WikipediaArticle, WikipediaRanking}

//val tp = Seq(("a", 1) ,("a" , 1),("b", 1))
//tp.groupBy(_._1).map(x=>(x._1,x._2.size))
//var rs=List()
//tp.reduce((a,b)=>rs++List((b)))

val conf: SparkConf = new SparkConf().setAppName("Simple Application").setMaster("spark://192.168.0.4:7077")
val sc: SparkContext = new SparkContext(conf)
// Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath,2).map(WikipediaData.parse(_))
val langs = List("Scala", "Java")
val articles = List(
  WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
  WikipediaArticle("2","Scala and Java run on the JVM"),
  WikipediaArticle("3","Scala is not purely functional")
)
val rdd = sc.parallelize(articles)
val rs = rdd.map(r=>{
  langs.map(l=>{
    if (r.mentionsLanguage(l)){
      l
    }
  })
})