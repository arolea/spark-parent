package io.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Local Spark configuration.
 * Created by rolea on 5/4/17.
 */
@Configuration
public class SparkConfig {

    /**
     * Creates the Spark configuration.
     */
    @Bean
    public SparkConf createSparkConfig(){
        return new SparkConf()
                // cluster name - run locally
                .setMaster("local")
                // application name
                .setAppName("My App");
    }

    /**
     * Creates the Java context.
     */
    @Bean
    public JavaSparkContext createJavaSparkContext() {
        JavaSparkContext context = new JavaSparkContext(createSparkConfig());
        context.setLogLevel("INFO");
        return context;
    }

    /**
     * Create the Spark SQL context.
     */
    @Bean
    public SQLContext createSQLContext(){
        return new SQLContext(createJavaSparkContext());
    }

}
