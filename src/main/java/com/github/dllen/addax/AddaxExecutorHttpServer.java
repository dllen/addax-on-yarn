package com.github.dllen.addax;

import com.google.common.collect.Maps;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AddaxExecutorHttpServer extends AbstractVerticle {

    private final String masterAddr;
    private final int port;
    private HttpServer server;

    static final AtomicInteger ID_GEN = new AtomicInteger();

    static int STATUS_DOING = 1;
    static int STATUS_FREE = 0;

    static final AtomicInteger EXECUTOR_STATUS = new AtomicInteger(STATUS_FREE);

    static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newScheduledThreadPool(1);
    static final Map<String, Object> HEARTBEAT_BODY = Maps.newHashMap();

    public AddaxExecutorHttpServer(String masterAddr, int port, String containerId) {
        this.masterAddr = masterAddr;
        this.port = port;
        HEARTBEAT_BODY.put("containerId", containerId);
        HEARTBEAT_BODY.put("port", port);

    }

    //定期上报心跳信息: port, containerId...
    public void startHeartbeatTask() {
        SCHEDULED_EXECUTOR_SERVICE.scheduleWithFixedDelay(() -> AddaxPigeon.INSTANCE.sendHeartbeat(HEARTBEAT_BODY, masterAddr), 10, 30, TimeUnit.SECONDS);
    }

    public void start() {
        String routePrefix = "/addax-executor/";
        server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        router.get(routePrefix + "status").respond(ctx -> Future.succeededFuture());
        router.post(routePrefix + "submit").respond(ctx -> {
            boolean canRun = EXECUTOR_STATUS.compareAndSet(STATUS_FREE, STATUS_DOING);
            if (canRun) {
                String jobConf = ctx.getBodyAsString();
                int jobId = ID_GEN.incrementAndGet();
                Thread t = new Thread(() -> {
                    Executor executor = new Executor(jobConf, masterAddr, jobId);
                    executor.run();
                    EXECUTOR_STATUS.compareAndSet(STATUS_DOING, STATUS_FREE);
                });
                t.start();
            }
            return Future.failedFuture("稍后重试！");
        });
        server.requestHandler(router).listen(port);
    }

    public void stop() {
        if (server != null) {
            server.close();
        }
    }

    static class Executor implements Runnable {

        private final String jobConf;
        private final String reportAddr;
        private final int jobId;

        public Executor(String jobConf, String reportAddr, int jobId) {
            this.jobConf = jobConf;
            this.reportAddr = reportAddr;
            this.jobId = jobId;
        }

        @Override
        public void run() {
            AddaxEngine addaxEngine = new AddaxEngine(reportAddr);
            addaxEngine.start(jobConf, jobId + "");
        }
    }
}
