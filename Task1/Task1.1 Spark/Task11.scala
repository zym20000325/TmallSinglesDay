import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

// 统计双十一最热门的商品

object Task11 {

  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Task11")
    val sc = new SparkContext(conf)

    // 读入log

    val log = sc.textFile(args(0))

    // 双十一
	 
    val log2 = log.filter(x=>x.split(",")(5).equals("1111"))

    // 添加购物车+购买+添加收藏夹

    val log3 = log2.filter(x=>x.split(",")(6).equals("0")==false)
	 
    // 降序 取前100位

    val item = log3.map(x=>(x.split(",")(1),1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1)).take(100)

    // 输出

    val result = item.foreach(println)

    sc.stop()

   }
}