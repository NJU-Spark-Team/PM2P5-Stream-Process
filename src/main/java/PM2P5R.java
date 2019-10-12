import component.AverageAccumulator;
import entity.Record;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.jackson.map.ObjectMapper;

import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

public class PM2P5R {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String[] fileName = new String[]{"Beijing_c", "Shanghai_c", "Guangzhou_c", "Chengdu_c", "Shenyang_c"};
    private static final String filePath = "/home/nosolution/Sundry/PM2.5 Data of Five Chinese Cities";
//    private static WebSocketClient client;

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: PM2P5R <source-hostname> <source-port>  <dest-hostname> <dest-port>");
            System.exit(1);
        }


        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH");

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("PM2P5R");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        // Create a JavaReceiverInputDStream on target ip:port and count the
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);

        JavaDStream<Record> records = lines.map(line -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(line, Record.class);
        });

        AverageAccumulator pmAverage = new AverageAccumulator();

        records.foreachRDD(rdd -> {
            rdd.foreach(record -> {
                pmAverage.add((double) record.getPm());
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
                String output = String.format("{ \"time\":\"%s\",  \"city name\":\"%s\", \"temperature\":%d, \"humidity\":%d, \"pm2.5\":%d, \"average pm2.5\":%f}",
                        formatter.format(record.getTime()), record.getName(), record.getTemp(), record.getHumi(), record.getPm(), pmAverage.value());
                //<deliver the output line to server>
                System.out.println(output);
            });
        });


    }

//    public void toTSRDD(Map<String, String> dataset, String start, String end) {
//        ZoneId defaultZone = ZoneId.systemDefault();
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-M-d H").withZone(defaultZone);
//        ZonedDateTime startDT = ZonedDateTime.parse(start, formatter);
//        ZonedDateTime endDT = ZonedDateTime.parse(end, formatter);
//
//        UniformDateTimeIndex dateTimeIndex = DateTimeIndexFactory.uniform(startDT, endDT, new HourFrequency(1), defaultZone);
//    }
}
