import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
                printFormatted("q1", element, String.valueOf(q1RDD.filter(row -> row.getRelationName().equals(element)).count())));

        return q1RDD;
    }

    private static void q2(JavaSparkContext sparkContext, JavaRDD<Relation> q1RDD) {
        SparkSession sparkSession = SparkSession.builder().sparkContext(sparkContext.sc()).getOrCreate();

        Dataset<Row> relationDF = sparkSession.createDataFrame(q1RDD, Relation.class);
        relationDF.createOrReplaceTempView("relations");

        long result = relationDF.sparkSession().sql("Select relationName from relations where relationName = 'R'").count();
        printFormatted("q21", String.valueOf(result));

        long result2 = relationDF.sparkSession().sql("Select relationName, attributeName, count(*) as countGroup " +
                "from (Select relationName, attributeName, attributeValue " +
                "from relations " +
                "group by relationName, attributeName, attributeValue) " +
                "group by relationName, attributeName " +
                "having countGroup > 1000").count();

        printFormatted("q22", String.valueOf(result2));

        Dataset<Row> results = relationDF.sparkSession().sql(
                "Select * " +
                        "from (Select relationName, attributeName, count(*) as countGroup " +
                        "from (Select relationName, attributeName, attributeValue " +
                        "from relations " +
                        "group by relationName, attributeName, attributeValue) " +
                        "group by relationName, attributeName " +
                        "order by countGroup " +
                        "limit 1) ");

        Dataset<String> namesDS = results.map((MapFunction<Row, String>) row -> row.getString(0) + "." + row.getString(1),
                Encoders.STRING());

        printFormatted("q23", namesDS.first());
    }

    private static void printFormatted(String question, String relationName) {
        System.out.printf(">> [%s: %s]", question, relationName);
        System.out.println();
    }

    private static void printFormatted(String question, String relationName, String result) {
        System.out.printf(">> [%s: %s: %s]", question, relationName, result);
        System.out.println();
    }

    private static void q3(JavaSparkContext sparkContext, JavaRDD<Relation> q1RDD) {
        JavaPairRDD<String, Integer> pairRDD = q1RDD.mapToPair((PairFunction<Relation, String, Integer>) s -> new Tuple2<>(s.getRelationName() + "." + s.getAttributeName(), s.getAttributeValue()));
        JavaPairRDD<String, ArrayList<Integer>> grouped = pairRDD.combineByKey(
                (integer) -> {
                    final ArrayList<Integer> list = new ArrayList<>();
                    list.add(integer);
                    return list;
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
        JavaPairRDD<Tuple2<String, ArrayList<Integer>>, Tuple2<String, ArrayList<Integer>>> cartesian = grouped.cartesian(grouped);
        JavaPairRDD<Tuple2<String, ArrayList<Integer>>, Tuple2<String, ArrayList<Integer>>> filtered = cartesian.filter(tuple -> {
            final Tuple2<String, ArrayList<Integer>> first = tuple._1;
            final Tuple2<String, ArrayList<Integer>> second = tuple._2;

            if (first._1.equals(second._1)) {
                return false;
            }

            return second._2.containsAll(first._2);
        });
        JavaRDD<Tuple2<String, String>> mapped = filtered.map(tuple -> new Tuple2<>(tuple._1._1, tuple._2._1));
        Iterable<Tuple2<String, String>> collected = mapped.collect();
        for (Tuple2<String, String> tuple : collected) {
            System.out.printf(">> [q3: %s,%s]", tuple._1, tuple._2);
            System.out.println();
        }

    }

    private static void q4(JavaSparkContext sparkContext, boolean onServer) {
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));
        javaStreamingContext.checkpoint("checkpoint");

		String hostname = (onServer) ? "stream-host" : "localhost";
        JavaReceiverInputDStream<String> lines =
                javaStreamingContext.socketTextStream(hostname, 9000);

        // TODO: Implement Q4 here.
        System.out.println(lines);

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
        
        windowedIPCounts.print();

        // JavaDStream<Long> totalWindowedIP = lines.countByWindow(Durations.seconds(20), Durations.seconds(4));

        AtomicLong totalCount = new AtomicLong();
        windowedIPCounts.foreachRDD(rdd -> {
            rdd.foreach(record -> {
                totalCount.addAndGet(record._2());
            });
        });

        // Iterate over all IP's and print when the relative frequency >= 3%
        windowedIPCounts.foreachRDD(rdd ->{
            rdd.foreach(record -> {
                if (record._2()/totalCount.intValue() >= 0.03) {
                    System.out.println(record._1());
                }
            });
        });

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

        q2(sparkContext, q1RDD);

        q3(sparkContext, q1RDD);

       q4(sparkContext, onServer);

        sparkContext.close();

    }
}
