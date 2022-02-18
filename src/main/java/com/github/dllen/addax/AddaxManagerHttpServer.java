package com.github.dllen.addax;

import com.github.dllen.ApplicationMaster;
import com.google.common.collect.Maps;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddaxManagerHttpServer extends AbstractVerticle {

    static final Logger LOGGER = LoggerFactory.getLogger(AddaxManagerHttpServer.class);

    private final int port;
    private final ApplicationMaster applicationMaster;
    private HttpServer server;

    public AddaxManagerHttpServer(int port, ApplicationMaster applicationMaster) {
        this.port = port;
        this.applicationMaster = applicationMaster;
    }

    public int getPort() {
        return port;
    }

    public void start() {
        String routePrefix = "/addax-server/";
        server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        router.get(routePrefix + "overview").respond(ctx -> Future.succeededFuture(Maps.newHashMap()));
        router.get(routePrefix + "status").respond(ctx -> Future.succeededFuture(Maps.newHashMap()));
        router.get(routePrefix + "cost").respond(ctx -> Future.succeededFuture(Maps.newHashMap()));
        router.get(routePrefix + "result").respond(ctx -> Future.succeededFuture(Maps.newHashMap()));
        router.post(routePrefix + "cancel").respond(ctx -> Future.succeededFuture());

        router.post(routePrefix + "submit").respond(ctx -> {
            String jobConf = ctx.getBodyAsString();
            LOGGER.warn("submit {}", jobConf);
            return Future.succeededFuture(Maps.newHashMap());
        });

        router.post(routePrefix + "heartbeat").respond(ctx -> {
            JsonObject body = ctx.getBodyAsJson();
            LOGGER.debug("heartbeat msg {}", body.toString());
            return Future.succeededFuture(Maps.newHashMap());
        });

        router.post(routePrefix + "report").respond(ctx -> {
            String status = ctx.getBodyAsString();
            LOGGER.warn("job status {}", status);
            return Future.succeededFuture(Maps.newHashMap());
        });

        server.requestHandler(router).listen(port);
    }

    public void stop() {
        if (server != null) {
            server.close();
        }
    }
}
