package io.service;

import org.apache.spark.api.java.JavaRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Example of processors.
 * Created by rolea on 5/8/17.
 */
@Component
public class StudentProcessor {

    @Autowired
    private StudentReader reader;

    /**
     * Example of using a DataFrame and a Row
     */
    public JavaRDD<String> processStudents(){
        return reader
                .readStudentsSparkSQL()
                .toJavaRDD()
                .map(row->"Student id : " + row.get(0) + ", student name : " + row.get(1) + ", student year : " + row.get(2));
    }

}
