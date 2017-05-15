package rdd.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Local Spark configuration.
 * Created by rolea on 5/4/17.
 */
@Configuration
public class SparkConfig {

    @Bean
    public SparkConf createSparkConfig(){
        return new SparkConf()
                .setMaster("local")
                .setAppName("RDD Processing App");
    }

    @Bean
    public JavaSparkContext createJavaContext() {
        return new JavaSparkContext(createSparkConfig());
    }

}
