package cluster.service;

import cluster.model.Product;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Examples of data generators.
 * Created by rolea on 5/5/17.
 */
@Component
public class ProductReader {

    @Autowired
    private JavaSparkContext context;

    private static final List<Product> FIRST_PRODUCT_LIST;
    static {
        FIRST_PRODUCT_LIST = new LinkedList<>();
        Random priceGenerator = new Random();
        for(int i = 0 ; i < 200 ; i ++ ) {
            FIRST_PRODUCT_LIST.add(new Product(i, "Product " + i, "Category " + i / 10, priceGenerator.nextDouble() * 200));
        }
    }

    public JavaPairRDD<String, Integer> getFirstProductsReducedByCategory() {
        return reduceByKeyExample(context.parallelize(FIRST_PRODUCT_LIST));
    }

    public JavaPairRDD<String, Integer> reduceByKeyExample(JavaRDD<Product> products) {
        return products
                .mapToPair(product->new Tuple2<>(product.getCategory(),1))
                .reduceByKey((count1,count2)->count1+count2)
                .partitionBy(new HashPartitioner(5))
                .persist(StorageLevel.MEMORY_ONLY());
    }

}
