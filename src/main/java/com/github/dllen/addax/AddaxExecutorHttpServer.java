package com.github.dllen.addax;

import com.github.dllen.addax.pojo.RespVo;
import com.github.dllen.utils.NetworkUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class AddaxExecutorHttpServer extends AbstractVerticle {

    static final AtomicInteger ID_GEN = new AtomicInteger();

    static final int STATUS_DOING = 1;
    static final int STATUS_FREE = 0;

    static final AtomicInteger EXECUTOR_STATUS = new AtomicInteger(STATUS_FREE);
    static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(4);
    static final Map<String, Object> HEARTBEAT_BODY = Maps.newHashMap();
    static final Logger LOGGER = LoggerFactory.getLogger(AddaxExecutorHttpServer.class);
    static final Timer TIMER = new Timer();

    private final String masterAddr;
    private final int port;
    private volatile AddaxPigeon addaxPigeon;


    public AddaxExecutorHttpServer(String masterAddr, int port, String containerId) {
        this.masterAddr = masterAddr;
        this.port = port;
        String host = NetworkUtils.getHostName();
        HEARTBEAT_BODY.put("containerId", containerId);
        HEARTBEAT_BODY.put("port", port);
        HEARTBEAT_BODY.put("host", host);
    }

    private AddaxPigeon getAddaxPigeon() {
        if (this.addaxPigeon == null) {
            synchronized (this) {
                if (this.addaxPigeon == null) {
                    this.addaxPigeon = new AddaxPigeon(vertx);
                }
            }
        }
        return this.addaxPigeon;
    }

    //定期上报心跳信息: port, containerId...
    private void initHeartbeatTask() {
        TIMER.schedule(new TimerTask() {
            @Override
            public void run() {
                HEARTBEAT_BODY.put("status", EXECUTOR_STATUS.get());
                HEARTBEAT_BODY.put("ts", System.currentTimeMillis());
                getAddaxPigeon().sendHeartbeat(HEARTBEAT_BODY, masterAddr);
            }
        }, 10000, 3000);
    }

    private void initHttpServer() {
        String routePrefix = "/addax-executor/";
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        router.route().handler(LoggerHandler.create()).handler(BodyHandler.create());

        router.get(routePrefix + "status").respond(ctx -> Future.succeededFuture());
        router.post(routePrefix + "submit").respond(ctx -> {
            boolean canRun = EXECUTOR_STATUS.compareAndSet(STATUS_FREE, STATUS_DOING);
            RespVo respVo = new RespVo();
            respVo.setCode(500);
            respVo.setMsg("error");
            if (canRun) {
                String jobConf = ctx.getBodyAsString();
                int jobId = ID_GEN.incrementAndGet();
                final ListenableFuture<String> listenableFuture = MoreExecutors.listeningDecorator(EXECUTOR_SERVICE).submit(new ExecutorWorker(jobConf, masterAddr, jobId));
                Futures.addCallback(listenableFuture, new FutureCallback<String>() {
                    @Override
                    public void onSuccess(String result) {
                        EXECUTOR_STATUS.compareAndSet(STATUS_DOING, STATUS_FREE);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        EXECUTOR_STATUS.compareAndSet(STATUS_DOING, STATUS_FREE);
                        LOGGER.error(t.getMessage(), t);
                    }
                });
                respVo.setData(ImmutableMap.of("jobId", jobId));
                respVo.setCode(200);
                respVo.setMsg("ok");
            }
            return Future.succeededFuture(respVo);
        });
        server.requestHandler(router).listen(port);
    }

    @Override
    public void start() throws Exception {
        initHeartbeatTask();
        initHttpServer();
    }

    @Override
    public void stop() throws Exception {
    }

    static class ExecutorWorker implements Callable<String> {

        private final String jobConf;
        private final String reportAddr;
        private final int jobId;

        public ExecutorWorker(String jobConf, String reportAddr, int jobId) {
            this.jobConf = jobConf;
            this.reportAddr = reportAddr;
            this.jobId = jobId;
        }

        @Override
        public String call() throws Exception {
            AddaxEngine addaxEngine = new AddaxEngine(reportAddr);
            addaxEngine.start(jobConf, jobId + "");
            return "ok";
        }
    }
}
