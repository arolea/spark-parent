package rdd.service.product;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import rdd.model.Product;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Pair RDD creation examples.
 * Created by rolea on 5/5/17.
 */
@Component
public class ProductReader {

    @Autowired
    private JavaSparkContext context;

    private static final List<Product> FIRST_PRODUCT_LIST;
    private static final List<Product> SECOND_PRODUCT_LIST;
    static {
        FIRST_PRODUCT_LIST = new LinkedList<>();
        SECOND_PRODUCT_LIST = new LinkedList<>();
        Random priceGenerator = new Random();

        for(int i = 0 ; i < 200 ; i ++ ) {
            FIRST_PRODUCT_LIST.add(new Product(i, "Product " + i, "Category " + i / 10, priceGenerator.nextDouble() * 200));
            if(i<100)
                SECOND_PRODUCT_LIST.add(new Product(i, "Product " + i, "Category " + i / 10, priceGenerator.nextDouble() * 200));
        }

    }

    public JavaPairRDD<String, Integer> getFirstProductsReducedByCategory() {
        return reduceByKeyExample(context.parallelize(FIRST_PRODUCT_LIST));
    }

    public JavaPairRDD<String, Integer> getSecondProductsReducedByCategory() {
        return reduceByKeyExample(context.parallelize(SECOND_PRODUCT_LIST));
    }

    public JavaRDD<Product> getProductRdd() {
        return context.parallelize(FIRST_PRODUCT_LIST);
    }

    /**
     * reduceByKey example - counts the number of products in each category.
     */
    public JavaPairRDD<String, Integer> reduceByKeyExample(JavaRDD<Product> products) {
        return products
                .mapToPair(product->new Tuple2<>(product.getCategory(),1))
                .reduceByKey((count1,count2)->count1+count2)
                // partitionBy example - split into 5 partitions based on hash values
                .partitionBy(new HashPartitioner(5))
                // persist this in memory for future lookups
                .persist(StorageLevel.MEMORY_ONLY());
    }

    /**
     * groupByKey example - groups products in categories.
     */
    public JavaPairRDD<String, Iterable<Integer>> groupByKeyExample(JavaRDD<Product> products) {
        return products
                .mapToPair(product->new Tuple2<>(product.getCategory(),1))
                .groupByKey();
    }

    /**
     * foldByKey example - counts the number of products in each category, starting from 2 instead on 1.
     */
    public JavaPairRDD<String, Integer> foldByKeyExample(JavaRDD<Product> products) {
        return products
                .mapToPair(product->new Tuple2<>(product.getCategory(),1))
                .foldByKey(2,(count1,count2)->count1+count2);
    }

    /**
     * combineByKey example - counts the number of products in each category.
     */
    public JavaPairRDD<String, Integer> combineByKeyExample(JavaRDD<Product> products) {
        return products
                .mapToPair(product->new Tuple2<>(product.getCategory(),1))
                .combineByKey(
                        // initial value for accumulator - invoked first time a key is processes (per partition)
                        (value)->1,
                        // adding a value to the accumulator - invoked when processing already encountered keys
                        (accumulator,value)->accumulator+value.intValue(),
                        // when merging results from different partitions - merges two accumulators
                        (accumulator1,accumulator2)->accumulator1+accumulator2,
                        // number of used threads
                        5);
    }

}
