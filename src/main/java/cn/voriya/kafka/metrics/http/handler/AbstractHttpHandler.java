package cn.voriya.kafka.metrics.http.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AbstractHttpHandler implements HttpHandler {
    @Override
    public final void handle(HttpExchange exchange) throws IOException {
        switch (exchange.getRequestMethod()) {
            case "GET":
                get(exchange, parseQuery(exchange));
                break;
            case "POST":
                post(exchange, parseQuery(exchange));
                break;
            case "DELETE":
                delete(exchange, parseQuery(exchange));
                break;
            case "PUT":
                put(exchange, parseQuery(exchange));
                break;
            default:
                exchange.getResponseBody().write((exchange.getRequestMethod() + " method are not allowed.").getBytes());
                break;
        }
        exchange.close();
    }

    protected void get(HttpExchange exchange, Map<String, String> params) throws IOException {
        exchange.getResponseBody().write("GET method are not allowed.".getBytes());
    }

    protected void post(HttpExchange exchange, Map<String, String> params) throws IOException {
        exchange.getResponseBody().write("POST method are not allowed.".getBytes());
    }

    protected void delete(HttpExchange exchange, Map<String, String> params) throws IOException {
        exchange.getResponseBody().write("DELETE method are not allowed.".getBytes());
    }

    protected void put(HttpExchange exchange, Map<String, String> params) throws IOException {
        exchange.getResponseBody().write("PUT method are not allowed.".getBytes());
    }

    private Map<String, String> parseQuery(HttpExchange exchange) {
        // 解析查询字符串
        Map<String, String> params = new HashMap<>();
        String query = exchange.getRequestURI().getQuery();
        if (StringUtils.isEmpty(query)) {
            return params;
        }
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            if (idx > 0) {
                String key = pair.substring(0, idx);
                String value = pair.substring(idx + 1);
                params.put(key, value);
            }
        }
        return params;
    }
}