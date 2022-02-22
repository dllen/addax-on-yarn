package com.github.dllen.test;

import com.github.dllen.addax.AddaxPigeon;
import com.google.common.collect.Maps;
import io.vertx.core.Vertx;

public class TestAddaxPigeon {

    public static void main(String[] args) throws InterruptedException {
        Vertx vertx = Vertx.vertx();
        AddaxPigeon addaxPigeon = new AddaxPigeon(vertx);
        addaxPigeon.sendGetRequest(9999, "172.20.100.115", "/", Maps.newHashMap());
        Thread.sleep(10000);
        vertx.close();
    }

}
