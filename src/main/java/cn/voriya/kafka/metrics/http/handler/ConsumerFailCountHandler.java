package cn.voriya.kafka.metrics.http.handler;

import cn.voriya.kafka.metrics.request.TopicConsumerOffset;
import cn.voriya.kafka.metrics.utils.JacksonUtil;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.util.Map;

public class ConsumerFailCountHandler extends AbstractHttpHandler {
    @Override
    protected void get(HttpExchange exchange, Map<String, String> params) throws IOException {
        exchange.sendResponseHeaders(200, 0);
        int minCount;
        try {
            minCount = Integer.parseInt(params.get("min-count"));
        } catch (Exception e) {
            minCount = 0;
        }
        int maxCount;
        try {
            maxCount = Integer.parseInt(params.get("max-count"));
        } catch (Exception e) {
            maxCount = Integer.MAX_VALUE;
        }
        exchange.getResponseBody().write(JacksonUtil.toJson(TopicConsumerOffset.getFailCount(minCount, maxCount)).getBytes());
    }
}




