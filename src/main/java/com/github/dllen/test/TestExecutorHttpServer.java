package com.github.dllen.test;

import com.github.dllen.addax.AddaxExecutorHttpServer;
import com.github.dllen.demo.DemoExecutorHttpServer;
import io.vertx.core.Vertx;

public class TestExecutorHttpServer {

    public static void main(String[] args) {
        //testVertx();
        testJetty();
    }


    public static void testVertx() {
        AddaxExecutorHttpServer addaxExecutorHttpServer = new AddaxExecutorHttpServer("127.0.0.1:9999", 9988, "12346");
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(addaxExecutorHttpServer);
    }

    public static void testJetty() {
        DemoExecutorHttpServer demoExecutorHttpServer = new DemoExecutorHttpServer();
        demoExecutorHttpServer.start("executor", 8090);
    }

}
