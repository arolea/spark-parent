package cluster;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Boot for Spark cluster mode.
 * Created by rolea on 5/12/17.
 */
@SpringBootApplication()
public class Application {

    @Autowired
    private JavaSparkContext context;

    public static void main(String[] args) {
        SpringApplication.run(Application.class,args);
    }

}
