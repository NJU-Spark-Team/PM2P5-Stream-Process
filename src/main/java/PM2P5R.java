import entity.Record;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.regex.Pattern;

public class PM2P5R {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: PM2P5R <source-hostname> <source-port>  <dest-hostname> <dest-port>");
            System.exit(1);
        }

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

        int[] rc = new int[1];

        records.foreachRDD(rdd->{
            rc[0]+=1;

        });



    }
}
