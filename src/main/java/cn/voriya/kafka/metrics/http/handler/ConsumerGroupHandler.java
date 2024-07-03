package cn.voriya.kafka.metrics.http.handler;

import cn.voriya.kafka.metrics.request.TopicConsumerOffset;
import cn.voriya.kafka.metrics.utils.JacksonUtil;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.util.Map;

public class ConsumerGroupHandler extends AbstractHttpHandler {
    @Override
    protected void get(HttpExchange exchange, Map<String, String> params) throws IOException {
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write(JacksonUtil.toJson(TopicConsumerOffset.getGroupsCache()).getBytes());
    }
}