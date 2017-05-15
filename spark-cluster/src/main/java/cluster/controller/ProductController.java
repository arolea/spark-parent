package cluster.controller;

import cluster.service.ProductProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Example of REST endpoint that triggers a Spark job.
 * Created by rolea on 5/12/17.
 */
@RestController
public class ProductController {

    @Autowired
    private ProductProcessor processor;

    @RequestMapping(value = "/getMapResult", method = RequestMethod.GET)
    public Map<String, Integer> testMap(){
        return processor.mapValuesExample().collectAsMap();
    }

}
