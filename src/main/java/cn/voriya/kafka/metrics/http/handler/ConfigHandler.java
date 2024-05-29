package cn.voriya.kafka.metrics.http.handler;


import cn.voriya.kafka.metrics.config.Config;
import cn.voriya.kafka.metrics.config.ConfigCluster;
import cn.voriya.kafka.metrics.utils.JacksonUtil;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.util.Map;

public class ConfigHandler extends AbstractHttpHandler {
    /**
     * 获取配置
     * query：无
     * body：无
     *
     * @see Config
     * 返回：Config
     */
    @Override
    public void get(HttpExchange exchange, Map<String, String> params) throws IOException {
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write(JacksonUtil.toJson(Config.getInstance()).getBytes());
    }

    /**
     * 更新/新增配置
     * query：无
     *
     * @see ConfigCluster
     * body：ConfigCluster
     * @see Config
     * 返回：修改后的Config
     */
    @Override
    public void post(HttpExchange exchange, Map<String, String> params) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes());
        ConfigCluster configCluster = JacksonUtil.toObject(body, ConfigCluster.class);
        Config.updateOrInsertCluster(configCluster);
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write(JacksonUtil.toJson(Config.getInstance()).getBytes());
    }

    /**
     * 删除配置
     *
     * @see String
     * query：clusterName
     * body：无
     * @see Config
     * 返回：修改后的Config
     */
    @Override
    public void delete(HttpExchange exchange, Map<String, String> params) throws IOException {
        String clusterName = params.get("clusterName");
        Config.removeCluster(clusterName);
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write(JacksonUtil.toJson(Config.getInstance()).getBytes());
    }
}
