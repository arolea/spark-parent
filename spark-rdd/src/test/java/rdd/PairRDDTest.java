package rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import rdd.service.product.ProductWriter;
import rdd.service.product.PairRddProductProcessor;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for PairRDD processors.
 * Created by rolea on 5/6/17.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class PairRDDTest {

    @Autowired
    private PairRddProductProcessor singleProcessor;

    @Autowired
    private ProductWriter writer;

    /**
     * Test for the map functionality.
     */
    @Test
    public void testMapValues(){
        JavaPairRDD<String, Integer> mappedCategories = singleProcessor.mapValuesExample();
        assertThat(mappedCategories).as("Squared categories count").isNotNull();
        writer.writePair(mappedCategories);
    }

    /**
     * Test for the flat map functionality.
     */
    @Test
    public void testFlatMapValues(){
        JavaPairRDD<String, Integer> flatMappedCategories = singleProcessor.flatMapValuesExample();
        assertThat(flatMappedCategories).as("Flattened categories count").isNotNull();
        writer.writePair(flatMappedCategories);
    }

    /**
     * Test for the sort functionality.
     */
    @Test
    public void testSortByKey(){
        JavaPairRDD<String, Integer> sortedCategories = singleProcessor.sortByKeysExample();
        assertThat(sortedCategories).as("Sorted categories").isNotNull();
        writer.writePair(sortedCategories);
    }

    /**
     * Test for the count by key functionality.
     */
    @Test
    public void testCountByKey() {
        Map<String, Long> groupCount = singleProcessor.countByKeyExample();
        assertThat(groupCount).as("Category count map not null").isNotNull();
        groupCount.forEach((key,value)->System.out.println("Key : " + key + ", value : " + value));
    }

    /**
     * Test for the reduce to map functionality.
     */
    @Test
    public void testReduceToMap(){
        Map<String, Integer> categoryMap = singleProcessor.collectToMapExample();
        assertThat(categoryMap).as("Category map not null").isNotNull();
        categoryMap.forEach((key,value)->System.out.println("Key : " + key + ", value : " + value));
    }

    /**
     * Test for the lookup functionality.
     */
    @Test
    public void testLookup(){
        List<Integer> values = singleProcessor.lookupExample();
        assertThat(values).as("Available values not null").isNotNull();
        values.forEach(System.out::println);
    }

}
