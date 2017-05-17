package rdd.service.product;

import org.apache.spark.api.java.JavaPairRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import javax.annotation.PostConstruct;

/**
 * Example of processors for set operations on PairRDDs.
 * Created by rolea on 5/5/17.
 */
@Component
public class SetPairRddProductProcessor {

    @Autowired
    private ProductReader reader;

    private JavaPairRDD<String, Integer> firstSet;
    private JavaPairRDD<String, Integer> secondSet;

    @PostConstruct
    public void init() {
        this.firstSet = reader.getFirstProductsCountReducedByCategory();
        this.secondSet = reader.getSecondProductsCountReducedByCategory();
    }

    /**
     * substractByKey example - removes from the first set the categories contained in the second set
     * (remove the keys contained in the second RDD from the first RDD).
     */
    public JavaPairRDD<String, Integer> substractExample(){
        return firstSet
                .subtractByKey(secondSet);
    }

    /**
     * join example - group category counts by matching category names (join two RDDs by common keys).
     */
    public JavaPairRDD<String, Tuple2<Integer, Integer>> joinExample(){
        return firstSet
                .join(secondSet);
    }

    /**
     * leftOuterJoin example - group category counts by matching category names (similar to SQL left outer join).
     */
    public JavaPairRDD<String, Tuple2<Integer, org.apache.spark.api.java.Optional<Integer>>> leftJoinExample(){
        return firstSet
                .leftOuterJoin(secondSet);
    }

    /**
     * rightOuterJoin example - group category counts by matching category names (similar to SQL right outer join).
     */
    public JavaPairRDD<String, Tuple2<org.apache.spark.api.java.Optional<Integer>, Integer>> rightJoinExample(){
        return firstSet
                .rightOuterJoin(secondSet);
    }

    /**
     * cogroup example - group category counts by matching category names (similar to SQL full outer join).
     */
    public JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroupExample(){
        return firstSet
                .cogroup(secondSet);
    }

}
