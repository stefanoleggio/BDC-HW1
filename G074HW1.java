
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

        //Spark setup

        SparkConf conf = new SparkConf(true).setAppName("G074HW1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        //Input reading

        int K = Integer.parseInt(args[0]); //Number of partitions
        int H = Integer.parseInt(args[1]); //Number of products
        String S = args[2]; //Country name
        String file_path = args[3]; //File path

        //Subdivide the input file into K partitions
        JavaRDD<String> rawData = sc.textFile(file_path).repartition(K).cache();

        //Task 1
        //Print the number of rows read from the input file
        System.out.println("Number of rows = " + rawData.count());
        System.out.println("TASK 1 DONE");

        //Task 2
        JavaPairRDD<String, Integer> productCustomer;

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

        System.out.println("Product-Customer Pairs = " + productCustomer.count());
        System.out.println("TASK 2 DONE");

        //Task3
        //Use mapPartitionsToPair/mapPartitions and combined with the groupByKey and mapValues or mapToPair/map methods

        JavaPairRDD<String, Integer> productPopularity1;

        productPopularity1 = productCustomer
                .mapPartitionsToPair((element) -> {    // <-- REDUCE PHASE (R1) (For each partition apply this function within the partition pairs)
                    HashMap<String, Integer> counts = new HashMap<>(); //hashmap which will contain the mapping PRODUCTID -> POPULARITY
                    while (element.hasNext()){ //count the popularity of each product iterating through elements of each singular partition
                        Tuple2<String, Integer> tuple = element.next();
                        counts.put(tuple._1(), 1 + counts.getOrDefault(tuple._1(), 0)); //increment of 1 the value of popularity within the partition
                    }
                    ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
                    for (Map.Entry<String, Integer> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));//add to the arraylist each distinct pair (productID, popularity) within the partition
                    }
                    return pairs.iterator(); //we have to return an iterator over the distinct pairs (productID, popularity)
                }).groupByKey()     // <-- SHUFFLE+GROUPING among different partitions to obtain a single partition
                .mapValues((it) -> { // <-- REDUCE PHASE (R2)
                    Integer sum = 0;
                    for (Integer c : it) { //sum up all the partial sums of product popularities
                        sum += c;
                    }
                    return sum; //return the final sum which will be applied to the values of the pairs (PRODUCTID, popularity <-)
                }); // Obs: one could use reduceByKey in place of groupByKey and mapValues

        System.out.println("TASK 3 DONE");

        //Task4: Repeats the operation of the previous point using a combination of map/mapToPair and reduceByKey methods
        JavaPairRDD<String, Integer> productPopularity2;

        productPopularity2 = productCustomer.mapToPair((elem) ->{
           return new Tuple2<String,Integer>(elem._1,1); //First we map (PRODUCTID, CUSTOMERID) into (PRODUCTID, 1) pairs:
        }).reduceByKey((x,y) -> x+y);; //then we reduce all the pairs (PRODUCTID, 1) summing all the occurences for each single key (PRODUCTID, sum())

        System.out.println("TASK 4 DONE");

        //Task5
        if(H > 0){
            JavaPairRDD<Integer,String> topElemets = productPopularity1.mapToPair(x -> new Tuple2<>(x._2, x._1)).sortByKey(false).repartition(1);
            System.out.println("Top " + H + " Products and their Popularities (using result of task 3)");
            for(Tuple2<Integer, String > element : topElemets.take(H)) {
                System.out.print("Product: " + element._2 + " ");
                System.out.println("Popularity: " + element._1 + ";");
            }
            topElemets = productPopularity2.mapToPair(x -> new Tuple2<>(x._2, x._1)).sortByKey(false).repartition(1);
            System.out.println("Top " + H +" Products and their Popularities (using result of task 4)");
            for(Tuple2<Integer, String > element : topElemets.take(H)) {
                System.out.print("Product: " + element._2 + " ");
                System.out.println("Popularity: " + element._1 + ";");
            }
            System.out.println("TASK 5 DONE");
        }//end if


        //Task6
        if(H == 0) {
            JavaPairRDD<String, Integer> Elements1 = productPopularity1.sortByKey().repartition(1);
            System.out.println("List of increasing lexicographic order of ProductID (using result of task3");
            for (Tuple2<String, Integer> element : Elements1.collect()) {
                System.out.print("Product: " + element._1 + " ");
                System.out.println("Popularity: " + element._2 + ";");
            }

            JavaPairRDD<String,Integer> Elements2 = productPopularity2.sortByKey().repartition(1);
            System.out.println("List of increasing lexicographic order of ProductID using result of task 4");
            for(Tuple2<String, Integer > element : Elements2.collect()) {
                System.out.print("Product: " + element._1 + " ");
                System.out.println("Popularity: " + element._2 + ";");
            }
            System.out.println("TASK 6 DONE");
        } //end if

    }
}