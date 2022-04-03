
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.io.IOException;
import java.util.*;

public class G074HW1{

    public static void main(String[] args) throws IOException {

        if (args.length != 4) { // Checking the number of the parameters
            throw new IllegalArgumentException("USAGE: num_partitions num_products country file_path");
        }


        /**
         *
         * Spark Setup
         *
         */

        SparkConf conf = new SparkConf(true).setAppName("G074HW1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        /**
         *
         * Input Reading
         *
         */

        int K = Integer.parseInt(args[0]); //Number of partitions
        int H = Integer.parseInt(args[1]); //Number of products
        String S = args[2]; //Country name
        String file_path = args[3]; //File path

        //Subdivide the input file into K partitions
        JavaRDD<String> rawData = sc.textFile(file_path).repartition(K).cache();

        //Task 1
        //Print the number of rows read from the input file
        System.out.println("Number of rows read from the input file = " + rawData.count());

        JavaPairRDD<String, Integer> productCustomer;

        //Task 2
        productCustomer = rawData
                .flatMapToPair((line) -> {
                    //Parsing
                    String[] tokens = line.split(",");//Split the line separating with ','
                    String productID = tokens[1];
                    int quantity = Integer.parseInt(tokens[3]);
                    String customerID = tokens[6];
                    String country = tokens[7];
                    ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
                    if((S.compareTo(country) == 0 | S.compareTo("all") == 0) & quantity > 0) { //Selecting the data with country equal to S and quantity > 0
                        //The following passage has been done because we had to avoid the use of distinct, so we had to do a kind of dummy map phase
                        //Create a pair with the key equals to the union of the two ID separated by a "-" : "productID-customerID"
                        //The value of the pair is zero in long format
                        pairs.add(new Tuple2<>(productID + "-" + customerID, 0));
                    }
                    return pairs.iterator();
                }).groupByKey().mapToPair(pair -> {
                    //After grouping the couples that have the same key, so same productID and customerID, We parse the key into productID and customerID
                    String[] tokens = pair._1.split("-");
                    return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
                });

        System.out.println("Number of rows after filtering = " + productCustomer.count());


        //Task3
        //Use mapPartitionsToPair/mapPartitions and combined with the groupByKey and mapValues or mapToPair/map methods

        JavaPairRDD<String, Integer> productPopularity1;

        productPopularity1 = productCustomer
                .mapPartitionsToPair((element) -> {    // <-- REDUCE PHASE (R1)
                    HashMap<String, Integer> counts = new HashMap<>();
                    while (element.hasNext()){
                        Tuple2<String, Integer> tuple = element.next();
                        counts.put(tuple._1(), 1 + counts.getOrDefault(tuple._1(), 0));
                    }
                    ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
                    for (Map.Entry<String, Integer> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                }).groupByKey()     // <-- SHUFFLE+GROUPING
                .mapValues((it) -> { // <-- REDUCE PHASE (R2)
                    Integer sum = 0;
                    for (Integer c : it) {
                        sum += c;
                    }
                    return sum;
                }); // Obs: one could use reduceByKey in place of groupByKey and mapValues

        System.out.println("Couple Popularity (ProductId -> Occurrence)");
        for (Tuple2<String, Integer> test : productPopularity1.take(10)) //or pairRdd.collect()
        {
            System.out.print(test._1 + " ");
            System.out.println(test._2);
        }


        //Task4

        //Task5
        System.out.println("Couple Popularity (ProductId -> Occurrence)");

        //Create comparator
        Comparator<Tuple2<String, Integer>> comparator = Comparator.comparing(Tuple2::_2);


        productPopularity1.takeOrdered(H, comparator).parallelStream().forEach((line)->
                System.out.println("Product "+line._1+" Occurrence "+line._2));



        //Task6

    }

}