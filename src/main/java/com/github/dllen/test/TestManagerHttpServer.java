package com.github.dllen.test;

import com.github.dllen.addax.AddaxManagerHttpServer;
import com.github.dllen.demo.DemoMasterJettyHttpServer;
import io.vertx.core.Vertx;

public class TestManagerHttpServer {

    public static void main(String[] args) {
        //testVertx();
        testJetty();
    }

    public static void testVertx() {
        AddaxManagerHttpServer addaxManagerHttpServer = new AddaxManagerHttpServer(9999, null);
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(addaxManagerHttpServer);
    }

    public static void testJetty() {
        DemoMasterJettyHttpServer demoMasterJettyHttpServer = new DemoMasterJettyHttpServer(null);
        demoMasterJettyHttpServer.start("http server", 9999);
    }
}
