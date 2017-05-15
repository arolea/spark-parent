package rdd.service.person;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import rdd.model.Person;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.LinkedList;
import java.util.List;

/**
 * RDD creation example.
 * Created by rolea on 5/4/17.
 */
@Component
public class PersonReader {

    @Autowired
    private JavaSparkContext context;

    private static final String PEOPLE_STRING;
    static {
        PEOPLE_STRING = "1,First person,20\n"+
                        "2,Second person,20\n"+
                        "3,Third person,20\n"+
                        "4,Forth person,21\n"+
                        "5,Fifth person,21";
    }

    /**
     * Converts a Java Collection to a RDD.
     */
    public JavaRDD<Person> getParallelizedPersonRdd(){
        List<Person> persons = new LinkedList<>();
        persons.add(new Person(1,"First person", 20));
        persons.add(new Person(2,"Second person", 21));
        persons.add(new Person(3,"Third person", 22));
        persons.add(new Person(4,"Forth person", 23));
        persons.add(new Person(5,"Fifth person", 24));
        return context.parallelize(persons);
    }

    /**
     * Converts an external data source to a RDD.
     */
    public JavaRDD<Person> getFilePersonRdd() throws Exception{
        File temp = File.createTempFile("pattern", ".suffix");
        temp.deleteOnExit();

        try (BufferedWriter out = new BufferedWriter(new FileWriter(temp))) {
            out.write(PEOPLE_STRING);
            out.close();
        } catch ( Exception e ) {
            e.printStackTrace();
        }

        // use a disk file for real applications
        // context.textFile("path");
        return context.textFile(temp.getPath()).map(line->{
            String tokens[] = line.split(",");
            return new Person(Integer.valueOf(tokens[0]),tokens[1],Integer.valueOf(tokens[2]));
        });

    }

}
