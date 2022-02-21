package com.github.dllen.addax;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;
import java.util.Map;

/**
 *
 */
public class AddaxPigeon {

    public static final AddaxPigeon INSTANCE = new AddaxPigeon();

    private final WebClient webClient;

    private AddaxPigeon() {
        Vertx vertx = Vertx.vertx(new VertxOptions());
        webClient = WebClient.create(vertx);
    }

    public void sendHeartbeat(Map<String, Object> data, String masterAddr) {
        String url = masterAddr + "heartbeat";
        webClient.post(url).sendJson(data);
    }

}
