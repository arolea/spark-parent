package rdd.service;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.springframework.stereotype.Component;
import rdd.model.Person;

/**
 * RDD writing example.
 * Created by rolea on 5/5/17.
 */
@Component
public class PersonWriter {

    public void writePeople(JavaRDD<Person> people){
        // use a disk file for real applications
        // counts.saveAsTextFile("path");
        people.foreach(person->System.out.println(person));
    }

    public void writeAges(JavaPairRDD<Integer,Integer> ageFrequencyTable){
        ageFrequencyTable.foreach(pair->System.out.println(pair));
    }

}
