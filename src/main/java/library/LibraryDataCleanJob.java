


package library;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// 图书馆实体类
class LibraryStat {
    public String libName;
    public int weekHour;
    public LibraryStat() {}
    public LibraryStat(String name, int hour) {
        this.libName = name;
        this.weekHour = hour;
    }
}

// 聚合累加器：新增visitor访客字段
class AvgAcc {
    public int sumHour = 0;
    public int count = 0;
    public String libName;
    public int visitor = 0;
}

// 公共主类，匹配文件名，无类找不到报错
public class LibraryDataCleanJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. HDFS数据源，随机循环发送数据
        DataStream<String> sourceStream = env.addSource(new RichSourceFunction<String>() {
            private volatile boolean runFlag = true;
            private List<String> data = new ArrayList<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // HDFS连接配置
                org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                conf.set("fs.defaultFS", "hdfs://192.168.102.128:9000");
                org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);

                org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/Library/raw/Library_data.csv");
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                String line;
                boolean isHeader = true;
                while ((line = reader.readLine()) != null) {
                    if (isHeader) {
                        isHeader = false;
                        continue;
                    }
                    if (!line.trim().isEmpty()) {
                        data.add(line);
                    }
                }
                reader.close();
                System.out.println("✅ 成功加载真实数据 " + data.size() + " 条记录！");
            }

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                Random rand = new Random();
                while (runFlag) {
                    int index = rand.nextInt(data.size());
                    ctx.collect(data.get(index));
                    Thread.sleep(500);
                }
            }

            @Override
            public void cancel() {
                runFlag = false;
            }
        });

        // 2. 清洗过滤表头、空数据
        SingleOutputStreamOperator<String> cleanStream = sourceStream.filter(line ->
                line != null && !line.trim().isEmpty() && !line.startsWith("NAME,HOURS"));

        // 3. 解析图书馆名称、周开放时长
        SingleOutputStreamOperator<LibraryStat> statStream = cleanStream.map(line -> {
            try {
                Pattern pattern = Pattern.compile(",(\\d+),[A-Z]+,\\d+$");
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    int weekHour = Integer.parseInt(matcher.group(1));
                    String name = line.split(",")[0].replaceAll("^\"|\"$", "").trim();
                    return new LibraryStat(name, weekHour);
                } else {
                    System.err.println("正则匹配失败: " + line);
                    return null;
                }
            } catch (Exception e) {
                System.err.println("解析失败: " + line + " 异常: " + e.getMessage());
                return null;
            }
        }).filter(stat -> stat != null);

        // 4. 【核心5秒滑动窗口】聚合：分馆名、总访客、平均开放时长
        DataStream<Tuple3<String, Integer, Double>> resultStream = statStream
                .keyBy(stat -> stat.libName)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(5)))
                .aggregate(new AggregateFunction<LibraryStat, AvgAcc, Tuple3<String, Integer, Double>>() {
                    @Override
                    public AvgAcc createAccumulator() {
                        return new AvgAcc();
                    }

                    @Override
                    public AvgAcc add(LibraryStat value, AvgAcc acc) {
                        acc.sumHour += value.weekHour;
                        acc.count += 1;
                        acc.libName = value.libName;
                        // 每条数据模拟30~300随机访客
                        Random random = new Random();
                        acc.visitor += random.nextInt(270) + 30;
                        return acc;
                    }

                    @Override
                    public Tuple3<String, Integer, Double> getResult(AvgAcc acc) {
                        double avgHour = acc.sumHour * 1.0 / acc.count;
                        return new Tuple3<>(acc.libName, acc.visitor, avgHour);
                    }

                    @Override
                    public AvgAcc merge(AvgAcc a, AvgAcc b) {
                        a.sumHour += b.sumHour;
                        a.count += b.count;
                        a.visitor += b.visitor;
                        return a;
                    }
                });

        // 5. 格式化写入HDFS：分馆名,访客总量,平均开放时长
        resultStream
                .map(t -> t.f0 + "," + t.f1 + "," + t.f2)
                .writeAsText("hdfs://192.168.102.128:9000/library/result/avg_week_hour.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        // 6. 控制台打印窗口实时结果（截图用）
        resultStream.print();

        env.execute("第四章-5秒滑动窗口-图书馆访客&开放时长实时聚合");
    }
}