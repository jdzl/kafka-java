package demo.kafka.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import demo.kafka.responseFormat.ResponseText;

@RestController
public class IndexController {
    @RequestMapping("")
    public ResponseText index(){
        return   new ResponseText ("Server running.. Apache kafka + Spring Boot");
    }
}
