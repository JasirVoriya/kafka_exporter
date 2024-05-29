package cn.voriya.kafka.metrics.http.handler;


import cn.voriya.kafka.metrics.config.Config;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.utils.JacksonUtil;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.util.Map;

public class ConfigHandler extends AbstractHttpHandler {
    @Override
    public void get(HttpExchange exchange, Map<String, String> params) throws IOException {
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write(JacksonUtil.toJson(Config.getInstance()).getBytes());
    }

    @Override
    public void post(HttpExchange exchange, Map<String, String> params) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes());
        ConfigCluster configCluster = JacksonUtil.toObject(body, ConfigCluster.class);
        Config.updateOrInsertCluster(configCluster);
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write(JacksonUtil.toJson(Config.getInstance()).getBytes());
    }

    @Override
    public void delete(HttpExchange exchange, Map<String, String> params) throws IOException {
        String clusterName = params.get("clusterName");
        Config.removeCluster(clusterName);
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write(JacksonUtil.toJson(Config.getInstance()).getBytes());
    }
}
