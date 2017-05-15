package io;

import io.model.Student;
import io.service.StudentReader;
import io.service.StudentWriter;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Spring Basic IO.
 * Created by rolea on 5/15/17.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class IOTest {

    @Autowired
    private StudentReader reader;

    @Autowired
    private StudentWriter writer;

    /**
     * Test basic IO functionality.
     */
    @Test
    @Ignore
    public void testJSON(){
        JavaRDD<Student> students = reader.readStudentsSparkBasic();
        assertThat(students).as("Students from JSON").isNotNull();
        writer.writeStudentsToJSON(students);
    }

}
