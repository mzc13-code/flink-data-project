package library

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.util.regex.{Matcher, Pattern}
import java.util.{ArrayList, List}
import scala.util.Random

// 改名，避开Java里的LibraryStat、AvgAcc
case class ScalaLibraryStat(libName: String, weekHour: Int)
case class ScalaAvgAcc(var sumHour: Int = 0, var count: Int = 0, var visitor: Int = 0, var libName: String = "")

object LibraryDataCleanScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val sourceStream = env.addSource(new RichSourceFunction[String] {
      @volatile private var runFlag = true
      private val data: List[String] = {
        val list = new ArrayList[String]()
        list.add("Vodak-East Side,\"Mon. & Wed., Noon-8; Tues. & Thurs., 10-6; Fri. & Sat., 9-5; Sun., 1-5\",3710 E. 106th St.,Chicago,IL,60617,(312) 747-5500,https://www.chipublib.org/locations/71/,\"(41.70283443594318, -87.61428978448026)\",48,MON,8")
        list.add("Albany Park,\"Mon. & Wed., 10-6; Tues. & Thurs., Noon-8; Fri. & Sat., 9-5; Sun., 1-5\",3401 W. Foster Ave.,Chicago,IL,60625,(773) 539-5450,https://www.chipublib.org/locations/3/,\"(41.97557881655979, -87.71361314512697)\",48,MON,8")
        list
      }

      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val rand = new Random()
        while (runFlag) {
          val line = data.get(rand.nextInt(data.size()))
          ctx.collect(line)
          Thread.sleep(500)
        }
      }

      override def cancel(): Unit = runFlag = false
    })

    val cleanStream = sourceStream.filter(line => line != null && line.trim.nonEmpty && !line.startsWith("NAME,"))

    val statStream = cleanStream.map(line => {
      try {
        val pattern: Pattern = Pattern.compile("(\\d+),[A-Z]+,\\d+$")
        val matcher: Matcher = pattern.matcher(line)
        if (matcher.find()) {
          val weekHour = matcher.group(1).toInt
          val name = line.split(",")(0).replaceAll("\"", "").trim
          ScalaLibraryStat(name, weekHour)
        } else {
          println(s"正则匹配失败：$line")
          null
        }
      } catch {
        case e: Exception =>
          println(s"解析失败：$line 异常：${e.getMessage}")
          null
      }
    }).filter(_ != null)

    val resultStream: DataStream[Tuple3[String, Integer, Double]] = statStream
      .keyBy((stat: ScalaLibraryStat) => stat.libName)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(5)))
      .aggregate(new AggregateFunction[ScalaLibraryStat, ScalaAvgAcc, Tuple3[String, Integer, Double]] {
        override def createAccumulator(): ScalaAvgAcc = ScalaAvgAcc()

        override def add(value: ScalaLibraryStat, acc: ScalaAvgAcc): ScalaAvgAcc = {
          acc.libName = value.libName
          acc.sumHour += value.weekHour
          acc.count += 1
          acc.visitor += new Random().nextInt(270) + 30
          acc
        }

        override def getResult(acc: ScalaAvgAcc): Tuple3[String, Integer, Double] = {
          val avgHour = acc.sumHour.toDouble / acc.count
          new Tuple3(acc.libName, acc.visitor, avgHour)
        }

        override def merge(a: ScalaAvgAcc, b: ScalaAvgAcc): ScalaAvgAcc = {
          a.sumHour += b.sumHour
          a.count += b.count
          a.visitor += b.visitor
          a
        }
      })

    resultStream.print()
    resultStream.map(t => s"${t.f0},${t.f1},${t.f2}")
      .writeAsText("./avg_week_hour.csv", FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)

    env.execute("Scala图书馆数据清洗窗口任务")
  }
}