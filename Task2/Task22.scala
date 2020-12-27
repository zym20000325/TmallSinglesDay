import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

// 统计双十一购买了商品的买家年龄段比例

object Task22 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Task22")
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

    // 小于18

    val age1 = info.filter(x=>(x.split(",")(1).equals("1"))).map(x=>(x.split(",")(0),1))
    val age1buyer = age1.subtractByKey(notbuyer).map(x=>("age1",1)).reduceByKey(_+_).map(x=>x._2)
    age1buyer.saveAsTextFile(args(2))

    // [18,24]

    val age2 = info.filter(x=>(x.split(",")(1).equals("2"))).map(x=>(x.split(",")(0),1))
    val age2buyer = age2.subtractByKey(notbuyer).map(x=>("age2",1)).reduceByKey(_+_).map(x=>x._2)
    age2buyer.saveAsTextFile(args(3))

    // [25,29]

    val age3 = info.filter(x=>(x.split(",")(1).equals("3"))).map(x=>(x.split(",")(0),1))
    val age3buyer = age3.subtractByKey(notbuyer).map(x=>("age3",1)).reduceByKey(_+_).map(x=>x._2)
    age3buyer.saveAsTextFile(args(4))

    // [30,34]

    val age4 = info.filter(x=>(x.split(",")(1).equals("4"))).map(x=>(x.split(",")(0),1))
    val age4buyer = age4.subtractByKey(notbuyer).map(x=>("age4",1)).reduceByKey(_+_).map(x=>x._2)
    age4buyer.saveAsTextFile(args(5))

    // [35,39]

    val age5 = info.filter(x=>(x.split(",")(1).equals("5"))).map(x=>(x.split(",")(0),1))
    val age5buyer = age5.subtractByKey(notbuyer).map(x=>("age5",1)).reduceByKey(_+_).map(x=>x._2)
    age5buyer.saveAsTextFile(args(6))

    // [40,49]

    val age6 = info.filter(x=>(x.split(",")(1).equals("6"))).map(x=>(x.split(",")(0),1))
    val age6buyer = age6.subtractByKey(notbuyer).map(x=>("age6",1)).reduceByKey(_+_).map(x=>x._2)
    age6buyer.saveAsTextFile(args(7))

    // 大于等于50

    val age7 = info.filter(x=>(x.split(",")(1).equals("7") || x.split(",")(1).equals("8"))).map(x=>(x.split(",")(0),1))
    val age7buyer = age7.subtractByKey(notbuyer).map(x=>("age7",1)).reduceByKey(_+_).map(x=>x._2)
    age7buyer.saveAsTextFile(args(8))

  
    sc.stop()

   }
}