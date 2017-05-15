package rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import rdd.service.product.SetPairRddProductProcessor;
import rdd.service.product.ProductWriter;
import scala.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for set operations on PairRDDs.
 * Created by rolea on 5/6/17.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class SetPairRDDTest {

    @Autowired
    private SetPairRddProductProcessor doubleProcessor;

    @Autowired
    private ProductWriter writer;

    /**
     * Test for substract operation.
     */
    @Test
    public void testSubstractByKey(){
        JavaPairRDD<String, Integer> substractedCategories = doubleProcessor.substractExample();
        assertThat(substractedCategories).as("Substracted categories").isNotNull();
        writer.writePair(substractedCategories);
    }

    /**
     * Test for join operation.
     */
    @Test
    public void testJoinByKey(){
        JavaPairRDD<String, Tuple2<Integer, Integer>> joinCategories = doubleProcessor.joinExample();
        assertThat(joinCategories).as("Join categories").isNotNull();
        writer.writePair(joinCategories);
    }

    /**
     * Test for left outer join operation.
     */
    @Test
    public void testLeftOuterJoin(){
        JavaPairRDD<String, Tuple2<Integer, org.apache.spark.api.java.Optional<Integer>>> joinCategories = doubleProcessor.leftJoinExample();
        assertThat(joinCategories).as("Left join categories").isNotNull();
        writer.writePair(joinCategories);
    }

    /**
     * Test for right outer join operation.
     */
    @Test
    public void testRightOuterJoin(){
        JavaPairRDD<String, Tuple2<org.apache.spark.api.java.Optional<Integer>, Integer>> joinCategories = doubleProcessor.rightJoinExample();
        assertThat(joinCategories).as("Right join categories").isNotNull();
        writer.writePair(joinCategories);
    }

    /**
     * Test for cogroup operation.
     */
    @Test
    public void testCogroup(){
        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroupCategories = doubleProcessor.cogroupExample();
        assertThat(cogroupCategories).as("Cogroup categories").isNotNull();
        writer.writePair(cogroupCategories);
    }

}
