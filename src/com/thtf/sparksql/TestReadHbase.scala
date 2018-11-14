package com.thtf.sparksql
 
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan, Result}
import org.apache.spark._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SQLContext}
import collection.JavaConverters._
 
case class NewUser(
    rowkey : String,
    name : String,
    age : Int,
    password : String)
/**
  * 案例样本
  * Hortonworks的spark on hbase方式读取hbase。
  * hbase版本不兼容。
  */
object TestReadHbase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
                        .setAppName("HBaseTest")
                        //本地测试
                        .setMaster("local")
                        //本地测试
                        .set("spark.sql.warehouse.dir", "C:/Users/Wyh/eclipse-workspace/BeijingMetroEnergy/spark-warehouse")
    val sc = new SparkContext(sparkConf)
 
    val tablename = "newuser"
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "stest")
    val hbaseContext = new HBaseContext(sc, conf)
    //创建一个扫描对象
    val scan = new Scan
    //val hbaseRdd = hbaseContext.hbaseRDD(TableName.valueOf(tablename), scan)
 
    // 建立一个数据库的连接
    val conn = ConnectionFactory.createConnection(conf);
    val hAdmin = conn.getAdmin
    if (tablename.equals("")) {
      println("Tablename can not be null !")
      return
    }
    if (!hAdmin.tableExists(TableName.valueOf(tablename))) {
      println(tablename + " is not exist");
      return
    }
 
    // 列族和列的set
    var resultSet = Set[String]()
    //获取表
    val table = conn.getTable(TableName.valueOf(tablename))
    // 扫描全表输出结果
    val results = table.getScanner(scan)
    val it: java.util.Iterator[Result] = results.iterator()
    while(it.hasNext) {
      val result = it.next()
      val cells = result.rawCells()
      for(cell <- cells) {
        print("行建:" + new String(CellUtil.cloneRow(cell)))
        print("列族:" + new String(CellUtil.cloneFamily(cell)))
        print("列名:" + new String(CellUtil.cloneQualifier(cell)))
        print("值:" + new String(CellUtil.cloneValue(cell)))
        print("时间戳:" + cell.getTimestamp())
        var str = Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell))
        resultSet += str
        println()
        println("---------------------")
      }
    }
    println(resultSet)
    
    // 各列对应的数据类型
    val columns_type = Map[String,String]("id" -> "string",
                           "name" -> "string",
                           "password" -> "string",
                           "age" -> "int")
 
    // 生成所有列scheme的方法
    def handleSet(rs : Set[String]): String = {
      var str = ""
      rs.foreach(r => {
        val arr = r.split(":", 2)
        //str += "\"" + r +"\":{\"cf\":\"" + arr(0) + "\", \"col\":\"" + arr(1) + "\", \"type\":\"string\"},"
        str += "\"" + arr(1) +"\":{\"cf\":\"" + arr(0) + "\", \"col\":\"" + arr(1) + "\", \"type\":\"" + columns_type.getOrElse(arr(1), "string") + "\"},"
      })
      // TODO 表中没有数据时，str为空，会报错
      str.substring(0, str.length-1)
    }
 
    // 获取列的scheme
    var columnsDes = handleSet(resultSet)
    println(columnsDes)
 
    // 创建表的scheme
    def catalog = s"""{
                      |"table":{"namespace":"default", "name":"${tablename}"},
                      |"rowkey":"key",
                      |"columns":{
                      |"rowkey":{"cf":"rowkey", "col":"key", "type":"${columns_type.getOrElse("id","string")}"},
                      |${columnsDes}
                      |}
                      |}""".stripMargin
 
    println("********" + catalog)
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
 
    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog -> cat))
        .format("org.apache.hadoop.hbase.spark")
        .load()
    }
    val df = withCatalog(catalog)
    df.show()
    val ds = df.as[NewUser]
    ds.show()
    ds.createTempView("newuser")
    sqlContext.sql("select * from newuser where age between 26 and 40").show()
    // 关闭资源
    results.close()
    table.close()
    conn.close()
 
    sc.stop()
  }
}
