package cluster.service;

import org.apache.spark.api.java.JavaPairRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Examples of processors.
 * Created by rolea on 5/5/17.
 */
@Component
public class ProductProcessor {

    @Autowired
    private ProductReader reader;

    private JavaPairRDD<String, Integer> productCountByCategory;

    @PostConstruct
    public void init(){
        productCountByCategory = reader.getFirstProductsReducedByCategory();
    }

    public JavaPairRDD<String, Integer> mapValuesExample() {
        return productCountByCategory
                .mapValues(categoryCount -> categoryCount * categoryCount);
    }

}
