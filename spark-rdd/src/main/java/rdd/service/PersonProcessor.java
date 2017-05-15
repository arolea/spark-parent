package rdd.service;

import org.apache.spark.api.java.JavaRDD;
import org.springframework.stereotype.Component;
import rdd.model.Person;

import java.util.Arrays;

/**
 * RDD processing example.
 * Created by rolea on 5/4/17.
 */
@Component
public class PersonProcessor {

    /**
     * Filter example (transformation) :
     */
    public JavaRDD<Person> getPeopleOver22(JavaRDD<Person> people) {
        return people
                .filter(person->person.getAge()>=22);
    }

    /**
     * Count example (action) :
     */
    public Long getPeopleOver22Count(JavaRDD<Person> people) {
        return getPeopleOver22(people)
                .count();
    }

    /**
     * Map example (transformation) :
     */
    public JavaRDD<Person> getDoubleAgePersonRdd(JavaRDD<Person> people) {
        return people
                .map(person->new Person(person.getId(),person.getName(),person.getAge()+person.getAge()));
    }

    /**
     * Flat map example (transformation) :
     */
    public JavaRDD<Person> getDuplicatePersonRdd(JavaRDD<Person> people){
        return people
                .flatMap(person-> Arrays.asList(person,person,person).iterator());
    }

}
