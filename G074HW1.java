
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

        JavaPairRDD<String, Long> productCustomer;

        //Task 2
        productCustomer = rawData
                .flatMapToPair((line) -> {
                    //Parsing
                    String[] tokens = line.split(",");//Split the line separating with ','
                    String productID = tokens[1];
                    int quantity = Integer.parseInt(tokens[3]);
                    String customerID = tokens[6];
                    String country = tokens[7];
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    if((S.compareTo(country) == 0 | S.compareTo("all") == 0) & quantity > 0) { //Selecting the data with country equal to S and quantity > 0
                        //The following passage has been done because we had to avoid the use of distinct, so we had to do a kind of dummy map phase
                        //Create a pair with the key equals to the union of the two ID separated by a "-" : "productID-customerID"
                        //The value of the pair is zero in long format
                        pairs.add(new Tuple2<>(productID + "-" + customerID, 0L));
                    }
                    return pairs.iterator();
                }).groupByKey().mapToPair(pair -> {
                    //After grouping the couples that have the same key, so same productID and customerID, We parse the key into productID and customerID
                    String[] tokens = pair._1.split("-");
                    return new Tuple2<>(tokens[0], Long.parseLong(tokens[1]));
                });

        System.out.println("Number of rows after filtering = " + productCustomer.count());


        //Task3

        //Task4

        //Task5

        //Task6

    }

}