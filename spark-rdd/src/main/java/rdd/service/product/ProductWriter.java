package rdd.service.product;

import org.apache.spark.api.java.JavaPairRDD;
import org.springframework.stereotype.Component;

/**
 * Dummy writers for testing purposes.
 * Created by rolea on 5/5/17.
 */
@Component
public class ProductWriter {

    public void writePair(JavaPairRDD<? extends Object, ? extends Object> categories) {
        categories.collectAsMap().forEach((key,value)->{
            System.out.println("Key : " + key + " , value : " + value);
        });
    }

}
