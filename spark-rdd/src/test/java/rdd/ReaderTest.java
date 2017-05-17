package rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import rdd.model.Product;
import rdd.service.product.ProductReader;
import rdd.service.product.ProductWriter;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for product readers.
 * Created by rolea on 5/6/17.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ReaderTest {

    @Autowired
    private ProductWriter writer;

    @Autowired
    private ProductReader reader;

    /**
     * Test for the reduce by key functionality.
     */
    @Test
    public void testReduceByKey(){
        JavaPairRDD<String, Integer> productCategoriesCount = reader.reduceByKeyExample(reader.getProductRdd());
        assertThat(productCategoriesCount).as("Product count per category").isNotNull();
        writer.writePair(productCategoriesCount);
    }

    /**
     * Test for the fold by key functionality.
     */
    @Test
    public void testFoldByKey(){
        JavaPairRDD<String, Integer> productCategoriesCount = reader.foldByKeyExample(reader.getProductRdd());
        assertThat(productCategoriesCount).as("Product count per category").isNotNull();
        writer.writePair(productCategoriesCount);
    }

    /**
     * Test for the combine by key functionality.
     */
    @Test
    public void testCombineByKey(){
        JavaPairRDD<String, Integer> productCategoriesCount = reader.combineByKeyExample(reader.getProductRdd());
        assertThat(productCategoriesCount).as("Product count per category").isNotNull();
        writer.writePair(productCategoriesCount);
    }

    /**
     * Test for the group by key functionality.
     */
    @Test
    public void testGroupByKey(){
        JavaPairRDD<String, Iterable<Product>> productsByCategory = reader.groupByKeyExample(reader.getProductRdd());
        assertThat(productsByCategory).as("Products per category").isNotNull();
        writer.writePair(productsByCategory);
    }

}
