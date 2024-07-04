package cn.voriya.kafka.metrics.http;

import cn.voriya.kafka.metrics.http.handler.ConfigHandler;
import cn.voriya.kafka.metrics.http.handler.ConsumerFailCountHandler;
import cn.voriya.kafka.metrics.http.handler.ConsumerGroupHandler;
import com.sun.net.httpserver.HttpServer;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;
import java.net.InetSocketAddress;

public class ExporterHttpServer {
    public static HTTPServer create(int port) throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(port), 3);
        httpServer.createContext("/config", new ConfigHandler());
        httpServer.createContext("/consumer-group", new ConsumerGroupHandler());
        httpServer.createContext("/consumer-fail-count", new ConsumerFailCountHandler());
        return new HTTPServer.Builder().withHttpServer(httpServer).build();
    }
}