package io.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.model.Student;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;

/**
 * Example of Spark readers.
 * Created by rolea on 5/8/17.
 */
@Component
public class StudentReader implements Serializable {

    @Autowired
    private JavaSparkContext javaContext;

    @Autowired
    private SQLContext sqlContext;

    private static ObjectMapper mapper = new ObjectMapper();

    /**
     * Reads the students from the JSON file via the Spark Basic API.
     */
    public JavaRDD<Student> readStudentsSparkBasic() {
        return javaContext
                .textFile("src/main/resources/JSON/students.in")
                .map(line -> mapper.readValue(line, Student.class));
    }

    /**
     * Reads the students from the JSON file via the Spark SQL API.
     */
    public Dataset<Row> readStudentsSparkSQL(){
        Dataset<Row> input = sqlContext.jsonFile("src/main/resources/JSON/students.in");
        input.registerTempTable("students");
        return sqlContext.sql("SELECT id, name, year FROM students ORDER BY year");
    }

}
