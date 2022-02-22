package com.github.dllen.addax;

import com.github.dllen.addax.pojo.Executor;
import com.github.dllen.addax.pojo.RespVo;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class AddaxPigeon {

    static final Logger LOGGER = LoggerFactory.getLogger(AddaxPigeon.class);

    private final WebClient webClient;

    public AddaxPigeon(Vertx vertx) {
        webClient = WebClient.create(vertx);
    }

    public void sendHeartbeat(Map<String, Object> data, String masterAddr) {
        String[] hostPort = masterAddr.split(":");
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);
        String uri = "/addax-server/heartbeat";
        LOGGER.debug("send heartbeat to {}", masterAddr);
        sendJsonData(port, host, uri, data);
    }

    public void submitJob(String jobConf, Executor executor) {
        String uri = "/addax-executor/submit";
        Future<HttpResponse<RespVo>> future = webClient.post(executor.getPort(), executor.getHost(), uri).as(BodyCodec.json(RespVo.class)).sendBuffer(Buffer.buffer(jobConf));
        future.onSuccess(resp -> LOGGER.debug("submit resp {}", resp)).onFailure(Throwable::printStackTrace);
    }

    public void sendJsonData(int port, String host, String uri, Map<String, Object> body) {
        webClient.post(port, host, uri).sendJson(body);
    }

    public void sendGetRequest(int port, String host, String uri, Map<String, String> params) {
        HttpRequest<Buffer> request = webClient.get(port, host, uri).as(BodyCodec.buffer());
        if (params != null && !params.isEmpty()) {
            params.forEach(request::addQueryParam);
        }
        Future<HttpResponse<Buffer>> future = request.send();
        future.onSuccess(resp -> System.out.println(resp.statusCode())).onFailure(Throwable::printStackTrace);
    }

}
