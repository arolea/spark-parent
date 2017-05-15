package cluster.config;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for Spark running in local cluster mode.
 * Created by rolea on 5/12/17.
 */
@Configuration
@ComponentScan(basePackages = {"pairs.service"})
public class ClusterConfig {

    @Bean
    public SparkConf createSparkConfig(){
        return new SparkConf()
                .setAppName("SparkCluster")
                .setJars(new String[] {"/home/rolea/git/java-learning/spark-parent/spark-cluster/target/spark-cluster-1.0-SNAPSHOT.jar"})
                .setMaster("spark://rolea-laptop:7077");
    }

    @Bean
    public JavaSparkContext createJavaContext() {
        return new JavaSparkContext(createSparkConfig());
    }

    /**
     * Broadcast a value.
     */
    @Bean
    public Broadcast<Map<String, String>> broadcastedMap(){
        JavaSparkContext context = createJavaContext();
        Map<String,String> table = new HashMap<>();
        table.put("A","B");
        table.put("C","D");
        table.put("E","F");
        Broadcast<Map<String, String>> broadcastedTable = context.broadcast(table);
        return broadcastedTable;
    }

    /**
     * Create an integer accumulator.
     */
    @Bean
    public Accumulator<Integer> createIntAccumulator() {
        return createJavaContext().accumulator(0, "myAccumulator");
    }

}
