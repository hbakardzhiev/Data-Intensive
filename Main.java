import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.util.ArrayList;

public class Main {

    private static JavaSparkContext getSparkContext(boolean onServer) {
        SparkConf sparkConf = new SparkConf().setAppName("2ID70-MS2");
        if (!onServer) sparkConf = sparkConf.setMaster("local[*]");
        return JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));
    }

    private static JavaRDD<String> q1(JavaSparkContext sparkContext, boolean onServer) {
        String databaseFilePath = (onServer) ? "/Database.csv" : "Database.csv";

        // TODO: You may change the value for the minPartitions parameter (template value 160) when running locally.
        // It is advised (but not compulsory) to set the value to 160 when running on the server.
        JavaRDD<String> databaseRDD = sparkContext.textFile(databaseFilePath, 160);

        // TODO: Implement Q1 here by defining q1RDD based on databaseRDD.
        JavaRDD<String> q1RDD = databaseRDD
            .flatMap((FlatMapFunction<String, String>) s -> {
            final ArrayList<String> list = new ArrayList<>();
            if (s.charAt(0) == '#') {
                return list.iterator();
            }
            // Split entries
            final String[] split = s.split(",");
            final String relation = split[0];
            final String[] attributes = split[1].split(";");
            final String[] values = split[2].split(";");

            // Add relation, attributes and values to list is requested format
            for (int i = 0; i < attributes.length; i++) {
                list.add(relation + "," + attributes[i] + "," + values[i]);
            }

            return list.iterator();
        });

        // Print results for automated testing
        System.out.println(">> [q1: R: " + q1RDD.filter(row -> 
            row.split(",")[0].equals("R")).count() + "]"); 
        System.out.println(">> [q1: S: " + q1RDD.filter(row -> 
            row.split(",")[0].equals("S")).count() + "]"); 
        System.out.println(">> [q1: T: " + q1RDD.filter(row -> 
            row.split(",")[0].equals("T")).count() + "]"); 

        return q1RDD;
    }

    private static void q2(JavaSparkContext sparkContext, JavaRDD<String> q1RDD) {
        SparkSession sparkSession = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();

        // TODO: Implement Q2 here.

    }

    private static void q3(JavaSparkContext sparkContext, JavaRDD<String> q1RDD) {
        JavaPairRDD<String, String> pairRDD = q1RDD.mapToPair((PairFunction<String, String, String>) s -> {
            final String[] split = s.split(",");
            final String letter = split[0];
            final String attribute = split[1];
            final String value = split[2];

            return new Tuple2<>(letter + "." + attribute, value);
        });
        JavaPairRDD<String, Iterable<String>> grouped = pairRDD.groupByKey();
        JavaPairRDD<Tuple2<String, Iterable<String>>, Tuple2<String, Iterable<String>>> cartesian = grouped.cartesian(grouped);
        JavaPairRDD<Tuple2<String, Iterable<String>>, Tuple2<String, Iterable<String>>> filtered = cartesian.filter((Function<Tuple2<Tuple2<String, Iterable<String>>, Tuple2<String, Iterable<String>>>, Boolean>) tuple -> {
            final Tuple2<String, Iterable<String>> first = tuple._1;
            final Tuple2<String, Iterable<String>> second = tuple._2;

            if (first._1.equals(second._1)) {
                return false;
            }

            for (String s1 : first._2) {
                boolean found = false;

                for (String s2 : second._2) {
                    if (s1.equals(s2)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false;
                }
            }
            return true;
        });
        JavaRDD<String> result = filtered.map((Function<Tuple2<Tuple2<String, Iterable<String>>, Tuple2<String, Iterable<String>>>, String>) tuple -> {
            final Tuple2<String, Iterable<String>> first = tuple._1;
            final Tuple2<String, Iterable<String>> second = tuple._2;


            return ">> [q3: " + first._1 + "," + second._1 + "]";
        });
        result.collect().forEach(System.out::println);
    }

    private static void q4(JavaSparkContext sparkContext, boolean onServer) {
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));
        javaStreamingContext.checkpoint("checkpoint");

        String hostname = (onServer) ? "stream-host" : "localhost";
        JavaReceiverInputDStream<String> lines = javaStreamingContext.socketTextStream(hostname, 9000);

        // TODO: Implement Q4 here.
        JavaPairDStream<String, Integer> windowedIPCounts = lines
            // Create count per ip address
            .flatMap(r -> {
                ArrayList<String> list = new ArrayList<>();
                String[] IPs = r.split(" ");
                for (String IP : IPs) {
                    list.add(IP);
                }
                return list.iterator();
            })
            .mapToPair(r -> new Tuple2<>(r, 1))
            // Reduce over the last 20 secs of data every 4 secs
            // I.e. sliding window = 20 and sliding interval = 4
            .reduceByKeyAndWindow((i1, i2) -> i1 + i2, Durations.seconds(20), Durations.seconds(4));

        JavaDStream<Long> totalWindowedIP = lines.countByWindow(Durations.seconds(20), Durations.seconds(4));

        // Iterate over all IP's and calculate the relative frequency?
        // End of TODO by the way

        // Start the streaming context, run it for two minutes or until termination
        javaStreamingContext.start();
        try {
            javaStreamingContext.awaitTerminationOrTimeout(2 * 60 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        javaStreamingContext.stop();
    }

    // Main method which initializes a Spark context and runs the code for each question.
    // To skip executing a question while developing a solution, simply comment out the corresponding method call.
    public static void main(String[] args) {

        boolean onServer = false; // TODO: Set this to true if and only if building a JAR to run on the server

        JavaSparkContext sparkContext = getSparkContext(onServer);

        JavaRDD<String> q1RDD = q1(sparkContext, onServer);

        // TODO: q2(sparkContext, q1RDD);

        q3(sparkContext, q1RDD);

        q4(sparkContext, onServer);

        sparkContext.close();

    }
}
