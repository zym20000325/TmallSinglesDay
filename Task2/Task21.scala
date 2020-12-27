import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

// 统计双十一购买了商品的男女比例

object Task21 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Task21")
    val sc = new SparkContext(conf)

    // 读入 log

    val log = sc.textFile(args(0))

    // 读入 info

    val info = sc.textFile(args(1))

    // 双十一
	 
    val log2 = log.filter(x=>x.split(",")(5).equals("1111"))

    // 购买

    val log3 = log2.filter(x=>x.split(",")(6).equals("2"))

    // 所有在双十一购买商品的 user 

    val buyer = log3.map(x=>(x.split(",")(0)))

    // 所有 user

    val all = log.map(x=>(x.split(",")(0),1))

    // 得到没有在双十一购买商品的 user

    val notbuyer = all.subtractByKey(buyer)

    // 男性

	val male = info.filter(x=>(x.split(",")(2).equals("1"))).map(x=>(x.split(",")(0),1))

    // 男性买家

    val malebuyer = male.subtractByKey(notbuyer).map(x=>("male",1)).reduceByKey(_+_).map(x=>x._2)
	malebuyer.saveAsTextFile(args(2))

    // 女性

    val female = info.filter(x=>(x.split(",")(2).equals("0"))).map(x=>(x.split(",")(0),1))

    // 女性买家

    val femalebuyer = female.subtractByKey(notbuyer).map(x=>("male",1)).reduceByKey(_+_).map(x=>x._2)
    femalebuyer.saveAsTextFile(args(2))

    sc.stop()

   }
}