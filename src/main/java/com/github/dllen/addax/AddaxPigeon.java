package com.github.dllen.addax;

import io.vertx.core.AbstractVerticle;
import io.vertx.ext.web.client.WebClient;
import java.util.Map;

public class AddaxPigeon extends AbstractVerticle {

    public static AddaxPigeon INSTANCE = new AddaxPigeon();

    WebClient webClient = WebClient.create(vertx);

    private AddaxPigeon() {
    }

    public void sendHeartbeat(Map<String, Object> data, String masterAddr) {
        String url = masterAddr + "heartbeat";
        webClient.post(url).sendJson(data);
    }

}
