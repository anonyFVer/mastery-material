package org.apache.dubbo.demo.consumer;

import org.apache.dubbo.demo.DemoService;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import java.util.concurrent.CompletableFuture;

public class Application {

    public static void main(String[] args) throws Exception {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("spring/dubbo-consumer.xml");
        context.start();
        DemoService demoService = context.getBean("demoService", DemoService.class);
        CompletableFuture<String> helloFuture = demoService.sayHelloAsync("world");
        System.out.println("result: " + helloFuture.get());
        System.in.read();
    }
}