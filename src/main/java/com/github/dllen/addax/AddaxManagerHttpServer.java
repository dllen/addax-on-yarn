package com.github.dllen.addax;

import com.github.dllen.ApplicationMaster;
import com.github.dllen.addax.pojo.Executor;
import com.github.dllen.addax.pojo.RespVo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class AddaxManagerHttpServer extends AbstractVerticle {

    static final Logger LOGGER = LoggerFactory.getLogger(AddaxManagerHttpServer.class);

    private final int port;
    private final ApplicationMaster applicationMaster;
    private final Map<String, Executor> executorMap = Maps.newConcurrentMap();
    private AddaxPigeon addaxPigeon;

    public AddaxManagerHttpServer(int port, ApplicationMaster applicationMaster) {
        this.port = port;
        this.applicationMaster = applicationMaster;
    }

    public int getPort() {
        return port;
    }

    private boolean doSubmitJob(String jobConf) {
        List<Executor> executors = executorMap.values().stream().filter(e -> e.getStatus() == 0).collect(Collectors.toList());
        int executorSize = executors.size();
        if (executorSize > 0) {
            int randomElementIndex = ThreadLocalRandom.current().nextInt(executorSize);
            Executor e = executors.get(randomElementIndex);
            addaxPigeon.submitJob(jobConf, e);
            return true;
        }
        return false;
    }

    private void initAddaxPigeon() {
        addaxPigeon = new AddaxPigeon(vertx);
    }

    private void initHttpServer() {
        String routePrefix = "/addax-server/";
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        router.route().handler(LoggerHandler.create()).handler(BodyHandler.create());

        router.get("/").respond(ctx -> {
            Map<String, Object> trackingData = Maps.newHashMap();
            int numContainers = applicationMaster.numTotalContainers;
            int numRunningContainers = applicationMaster.getRunningContainers().size();
            List<Map<String, Object>> runningContainers = Lists.newArrayList();
            for (Map.Entry<ContainerId, Container> e : applicationMaster.getRunningContainers().entrySet()) {
                Map<String, Object> containerData = Maps.newHashMap();
                containerData.put("containerId", e.getKey().getContainerId());
                containerData.put("applicationAttemptId", e.getKey().getApplicationAttemptId().toString());
                containerData.put("nodeId", e.getValue().getNodeId().toString());
                containerData.put("nodeHttpAddress", e.getValue().getNodeHttpAddress());
                containerData.put("mem", e.getValue().getResource().getMemory());
                containerData.put("vcores", e.getValue().getResource().getVirtualCores());
                containerData.put("nodeLinks", getNodeLinks(e.getValue().getNodeHttpAddress()));
                runningContainers.add(containerData);
            }
            trackingData.put("numContainers", numContainers);
            trackingData.put("numRunningContainers", numRunningContainers);
            trackingData.put("runningContainers", runningContainers);
            return Future.succeededFuture(trackingData);
        });

        router.get(routePrefix + "overview").respond(ctx -> Future.succeededFuture(Maps.newHashMap()));
        router.get(routePrefix + "status").respond(ctx -> Future.succeededFuture(Maps.newHashMap()));
        router.get(routePrefix + "cost").respond(ctx -> Future.succeededFuture(Maps.newHashMap()));
        router.get(routePrefix + "result").respond(ctx -> Future.succeededFuture(Maps.newHashMap()));
        router.post(routePrefix + "cancel").respond(ctx -> Future.succeededFuture());

        router.post(routePrefix + "submit").respond(ctx -> {
            String jobConf = ctx.getBodyAsString();
            LOGGER.debug("submit {}", jobConf);
            boolean ok = doSubmitJob(jobConf);
            RespVo respVo = new RespVo();
            if (ok) {
                respVo.setCode(200);
                respVo.setMsg("ok");
            } else {
                respVo.setCode(500);
                respVo.setMsg("error");
            }
            return Future.succeededFuture(respVo);
        });

        router.post(routePrefix + "heartbeat").respond(ctx -> {
            JsonObject body = ctx.getBodyAsJson();
            LOGGER.debug("executor heartbeat msg {}", body.toString());
            int port = body.getInteger("port");
            String containerId = body.getString("containerId");
            int status = body.getInteger("status");
            String host = body.getString("host");
            Executor executor = new Executor(host, port, status, containerId);
            executorMap.put(containerId, executor);
            return Future.succeededFuture(Maps.newHashMap());
        });

        router.post(routePrefix + "report").respond(ctx -> {
            String status = ctx.getBodyAsString();
            LOGGER.debug("job status {}", status);
            return Future.succeededFuture(Maps.newHashMap());
        });

        server.requestHandler(router).listen(port);
    }

    @Override
    public void start() throws Exception {
        initAddaxPigeon();
        initHttpServer();
    }

    @Override
    public void stop() throws Exception {
    }

    private String getNodeLinks(String nodeHttpAddress) {
        return "http://" + nodeHttpAddress + "/node/application/" + applicationMaster.appAttemptID.getApplicationId();
    }

}
