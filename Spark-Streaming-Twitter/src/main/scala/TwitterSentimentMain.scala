import java.io._

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterSentimentMain {

  def main(args: Array[String]) {


    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("STweetsApp").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val path = "${path_to_saved_tweets}"
    val abc = sqlContext.read.json(path)

    abc.printSchema()
    abc.registerTempTable("def")
    val lang = sqlContext.sql("SELECT lang,count(*) AS cnt FROM def WHERE lang <> 'null' GROUP BY lang ORDER BY cnt DESC LIMIT 10")
    val c=lang.collect()
    val pr = new PrintWriter("${path_to_print}")
      pr.println("letter  frequency")
    for (i<-0 to c.length-1)
    {
      pr.println(c{i})
    }
    pr.close()



    val rcount = sqlContext.sql("SELECT retweeted_status.text,retweeted_status.retweet_count  FROM def ORDER BY retweeted_status.retweet_count DESC LIMIT 10")
    val rc = new PrintWriter("F:/topretweet.txt")
    val r=rcount.collect()
    for (i<-0 to r.length-1)
    {
      rc.println(r{i})
    }
    rc.close()

    val fcount = sqlContext.sql("SELECT retweeted_status.text,retweeted_status.favorite_count  FROM def ORDER BY retweeted_status.favorite_count DESC LIMIT 10")
    val fc = new PrintWriter("${path_to_print}")
    val f=fcount.collect()
    for (i<-0 to f.length-1)
    {
      rc.println(f{i})
    }

    val frcount = sqlContext.sql("SELECT user.name,user.friends_count  FROM def ORDER BY user.friends_count DESC LIMIT 10")
    val frc = new PrintWriter("${path_to_print}")
    val fr=frcount.collect()
    for (i<-0 to fr.length-1)
    {
      frc.println(fr{i})
    }
    val focount = sqlContext.sql("SELECT user.name,user.followers_count  FROM def ORDER BY user.followers_count DESC LIMIT 1")
    val foc = new PrintWriter("${path_to_print}")
    val fo=focount.collect()
    for (i<-0 to fo.length-1)
    {
      foc.println(fo{i})
    }
  }





}
