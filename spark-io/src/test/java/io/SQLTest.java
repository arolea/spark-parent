package io;

import io.service.StudentProcessor;
import io.service.StudentReader;
import io.service.StudentWriter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Spark SQL module.
 * Created by rolea on 5/15/17.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class SQLTest {

    @Autowired
    private StudentReader reader;

    @Autowired
    private StudentProcessor processor;

    @Autowired
    private StudentWriter writer;

    public SQLTest() {
    }

    /**
     * Test reads from JSON via the SQL API.
     */
    @Test
    public void testReader(){
        Dataset<Row> students = reader.readStudentsSparkSQL();
        assertThat(students).as("Students data set is not null").isNotNull();
        students.collectAsList().forEach(System.out::println);
    }

    /**
     * Test processing on Row objects.
     */
    @Test
    public void testProcessor(){
        JavaRDD<String> students = processor.processStudents();
        assertThat(students).as("Students rdd is not null").isNotNull();
        students.collect().forEach(System.out::println);
    }

    /**
     * Test writes via the SQL API.
     */
    @Test
    @Ignore
    public void testWriter(){
        writer.writeStudents();
    }

}
