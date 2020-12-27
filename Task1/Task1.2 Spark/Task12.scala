import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

// 最受年轻人(age<30)关注的商家

object Task12 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Task12")
    val sc = new SparkContext(conf)

    // 读入log

    val log = sc.textFile(args(0))

    // 读入预处理过的 info 仅包含所有 age_range 不是 1，2，3 的 user_id 

    val info = sc.textFile(args(1))

    // 双十一
	 
    val log2 = log.filter(x=>x.split(",")(5).equals("1111"))

    // 添加购物车+购买+添加收藏夹

    val log3 = log2.filter(x=>x.split(",")(6).equals("0")==false)

    // 所有 user 及其选择的 seller

    val all = log3.map(x=>(x.split(",")(0),x.split(",")(3)))

    // 非年轻人

    val old = info.map(x=>(x.split(",")(0),1))

    // 在所有 user 中去除非年轻人 筛选出年轻人对应的 seller

    val young = all.subtractByKey(old).map(x=>(x._2, 1)).reduceByKey(_+_)
	 
    // 降序 取前100位

    val seller = young.map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1)).take(100)

    // 输出

    val result = seller.foreach(println)

    sc.stop()

   }
}