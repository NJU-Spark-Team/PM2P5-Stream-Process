import com.cloudera.sparkts.models.HoltWinters;
import com.cloudera.sparkts.models.HoltWintersModel;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.time.format.DateTimeFormatter;

public class PM2P5S {
    //    private static final Pattern SPACE = Pattern.compile(" ");
//    private static final String[] fileName = new String[]{"Beijing_c", "Shanghai_c", "Guangzhou_c", "Chengdu_c", "Shenyang_c"};
//    private static final String filePath = "file:///home/nosolution/Sundry/PM2.5 Data of Five Chinese Cities";
    private static OkHttpClient client;

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: pm2p5s <source-hostname> <source-port>  <dest-hostname> <dest-port>");
            System.exit(1);
        }

        client = new OkHttpClient();
        final String url = "http://" + args[2] + ":" + args[3] + "/submit";

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH");

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("PM2P5R").setMaster("local[4]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        // Create a JavaReceiverInputDStream on target ip:port and count the
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);

//        JavaDStream<String> lines = ssc.textFileStream(filePath + "/test.txt" );

//        JavaDStream<Record> records = lines.map(line -> {
//            ObjectMapper mapper = new ObjectMapper();
//            System.out.println(line);
//            return mapper.readValue(line, Record.class);
//        });


        JavaPairDStream<String, Integer> m = lines.mapToPair(line -> new Tuple2<>(line, 1));

        m.foreachRDD(rdd -> {
            rdd.foreach(p -> {
                System.out.println("a");
            });

        });


//        m.foreachRDD(rdd -> {
//            if (!rdd.isEmpty()) {
//                rdd.foreach(line -> {
//                pmAverage.add((double) record.getPm());
                /*
                json format:
                {
                    "time": str,
                    "city name"： str,
                    "temperature": number,
                    "humidity": number,
                    "pm2.5": number,
                    "average pm2.5": number
                }
                 */
//                String output = String.format("{ \"time\":\"%s\",  \"city name\":\"%s\", \"temperature\":%d, \"humidity\":%d, \"pm2.5\":%d, \"average pm2.5\":%f}",
//                        formatter.format(record.getTime()), record.getName(), record.getTemp(), record.getHumi(), record.getPm(), pmAverage.value());
        //<deliver the output line to server>
//                });
//
//            }
//        });

        ssc.start();
        ssc.awaitTermination();


    }

    /**
     * post method to server
     *
     * @param url     dest url
     * @param jsonStr String representation of json object
     */
    private void post(String url, String jsonStr) {
        RequestBody body = RequestBody.create(jsonStr, MediaType.parse("application/json; charset=utf-8"));
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        try {
            client.newCall(request).execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 使用Holt-Winter模型对时间序列进行你和
     *
     * @param ts         time series, 按时间排列的属性值，本项目中为pm2.5
     * @param predictedN 预测值的个数
     * @return 按顺序排列的预测值
     */
    private Vector forecast(Vector ts, int predictedN) {

        HoltWintersModel model = HoltWinters.fitModelWithBOBYQA(ts, 24, "additive");
        Vector dest = Vectors.zeros(predictedN);
        model.forecast(ts, dest);

        return dest;
    }


}
