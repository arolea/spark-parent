package io.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.model.Student;
import org.apache.spark.api.java.JavaRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

/**
 * Example of writers.
 * Created by rolea on 5/8/17.
 */
@Component
public class StudentWriter {

    @Autowired
    private StudentReader reader;

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Example of writing via the basic Spark API.
     * @param values
     */
    public void writeStudentsToJSON(JavaRDD<Student> values) {
        values
                .mapPartitions(students -> {
                    List<String> text = new LinkedList<>();
                    while (students.hasNext()) {
                        text.add(mapper.writeValueAsString(students.next()));
                    }
                    return text.iterator();})
                .saveAsTextFile("students.out");
    }

    /**
     * Example of writing via the Spark SQL API.
     */
    public void writeStudents(){
        reader
                .readStudentsSparkSQL()
                .write()
                .mode("overwrite")
                .json("temp");
    }

}
