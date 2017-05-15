package rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import rdd.model.Person;
import rdd.service.PersonProcessor;
import rdd.service.PersonReader;
import rdd.service.PersonWriter;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for RDD operations.
 * Created by rolea on 5/5/17.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class PersonRDDTest {

    @Autowired
    private PersonReader reader;

    @Autowired
    private PersonProcessor processor;

    @Autowired
    private PersonWriter writer;

    @Autowired
    private JavaSparkContext context;

    private JavaRDD<Person> listPeople;
    private JavaRDD<Person> filePeople;

    @Before
    public void initRDD()throws Exception{
        this.listPeople = reader.getParallelizedPersonRdd();
        this.filePeople = reader.getFilePersonRdd();
    }

    /**
     * Test for filter functionality.
     */
    @Test
    public void testFilter(){
        JavaRDD<Person> filtered = processor.getPeopleOver22(listPeople);
        assertThat(filtered).as("People over 22").isNotNull();
        writer.writePeople(filtered);
    }

    /**
     * Test for count functionality.
     */
    @Test
    public void testCount(){
        Long count = processor.getPeopleOver22Count(listPeople);
        assertThat(count).as("People over 22 count").isEqualTo(3);
    }

    /**
     * Test for map functionality.
     */
    @Test
    public void testMap(){
        JavaRDD<Person> doubleAgedPeople = processor.getDoubleAgePersonRdd(filePeople);
        assertThat(doubleAgedPeople).as("Double aged filePeople RDD").isNotNull();
        writer.writePeople(doubleAgedPeople);
    }

    /**
     * Test for flatMap functionality.
     */
    @Test
    public void testFlatMap(){
        JavaRDD<Person> duplicatePeople = processor.getDuplicatePersonRdd(filePeople);
        assertThat(duplicatePeople).as("Duplicate filePeople RDD").isNotNull();
        writer.writePeople(duplicatePeople);
    }

}
