package cn.voriya.kafka.metrics.http.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Log4j2
public class AbstractHttpHandler implements HttpHandler {
    @Override
    public final void handle(HttpExchange exchange) {
        try {
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
                    this.methodNotFollowed(exchange);
                    break;
            }
        } catch (Throwable t) {
            log.error("error:", t);
            try {
                if (exchange.getResponseCode() == -1) {
                    exchange.sendResponseHeaders(500, 0);
                }
                exchange.getResponseBody().write(t.getMessage().getBytes());
            } catch (IOException e) {
                log.error("error:", e);
            }
        } finally {
            exchange.close();
        }
    }

    protected void get(HttpExchange exchange, Map<String, String> params) throws IOException {
        this.methodNotFollowed(exchange);
    }

    protected void post(HttpExchange exchange, Map<String, String> params) throws IOException {
        this.methodNotFollowed(exchange);
    }

    protected void delete(HttpExchange exchange, Map<String, String> params) throws IOException {
        this.methodNotFollowed(exchange);
    }

    protected void put(HttpExchange exchange, Map<String, String> params) throws IOException {
        this.methodNotFollowed(exchange);
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
    private void methodNotFollowed(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().write((exchange.getRequestMethod() + " method are not allowed.").getBytes());
    }
}
