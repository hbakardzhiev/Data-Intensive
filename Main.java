import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.util.*;

public class Main {

    private static JavaSparkContext getSparkContext(boolean onServer) {
        SparkConf sparkConf = new SparkConf().setAppName("2ID70-MS2");
        if (!onServer) sparkConf = sparkConf.setMaster("local[*]");
        return JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));
    }

    private static JavaRDD<Relation> q1(JavaSparkContext sparkContext, boolean onServer) {
        String databaseFilePath = (onServer) ? "/Database.csv" : "Database.csv";

        // TODO: You may change the value for the minPartitions parameter (template value 160) when running locally.
        // It is advised (but not compulsory) to set the value to 160 when running on the server.
        JavaRDD<String> databaseRDD = sparkContext.textFile(databaseFilePath, 160);

        // TODO: Implement Q1 here by defining q1RDD based on databaseRDD.
        JavaRDD<Relation> q1RDD = databaseRDD
                .flatMap((FlatMapFunction<String, Relation>) s -> {
                    final ArrayList<Relation> list = new ArrayList<>();
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
                        final Relation currRelation = new Relation(relation, attributes[i], Integer.parseInt(values[i]));
                        list.add(currRelation);
                    }

                    return list.iterator();
                });

        final String[] relationNames = {"R", "S", "T"};
        Arrays.stream(relationNames).forEach(element ->
                printFormatted("q1", element, String.valueOf(q1RDD.filter(row ->
                        row.getRelationName().equals(element)).count())));

        return q1RDD;
    }

    private static void q2(JavaSparkContext sparkContext, JavaRDD<String> q1RDD) {
        SparkSession sparkSession = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();


//        Dataset<Row> relationDF = sparkSession.createDataFrame(q2RDD, Relation.class);
//        relationDF.createOrReplaceTempView("relations");


    }


    private static void printFormatted(String question, String relationName, String result) {
        System.out.printf(">> [%s: %s %s]", question, relationName, result);
        System.out.println();
    }

    private static void q3(JavaSparkContext sparkContext, JavaRDD<Relation> q1RDD) {
        JavaPairRDD<String, Integer> pairRDD = q1RDD.mapToPair((PairFunction<Relation, String, Integer>) s -> new Tuple2<>(s.getRelationName() + "." + s.getAttributeName(), s.getAttributeValue()));
        JavaPairRDD<String, TreeSet<Integer>> grouped = pairRDD.combineByKey(
                (integer) -> {
                    final TreeSet<Integer> treeSet = new TreeSet<>();
                    treeSet.add(integer);
                    return treeSet;
                },
                (list, integer) -> {
                    list.add(integer);
                    return list;
                },
                (list, list2) -> {
                    list.addAll(list2);
                    return list;
                }

        );
        JavaPairRDD<Tuple2<String, TreeSet<Integer>>, Tuple2<String, TreeSet<Integer>>> cartesian = grouped.cartesian(grouped);
        JavaPairRDD<Tuple2<String, TreeSet<Integer>>, Tuple2<String, TreeSet<Integer>>> filtered = cartesian.filter(tuple -> {
            final Tuple2<String, TreeSet<Integer>> first = tuple._1;
            final Tuple2<String, TreeSet<Integer>> second = tuple._2;

            if (first._1.equals(second._1)) {
                return false;
            }

            return second._2.containsAll(first._2);
        });
        Iterable<Tuple2<Tuple2<String, TreeSet<Integer>>, Tuple2<String, TreeSet<Integer>>>> collected = filtered.collect();
        for (Tuple2<Tuple2<String, TreeSet<Integer>>, Tuple2<String, TreeSet<Integer>>> tuple : collected)
            printFormatted("q3", tuple._1._1, tuple._2._1);

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
                .reduceByKeyAndWindow(Integer::sum, Durations.seconds(20), Durations.seconds(4));

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

        JavaRDD<Relation> q1RDD = q1(sparkContext, onServer);


//        q2(sparkContext, q1RDD);

        q3(sparkContext, q1RDD);

//        q4(sparkContext, onServer);

        sparkContext.close();

    }
}
