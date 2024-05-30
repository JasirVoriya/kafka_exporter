package cn.voriya.kafka.metrics.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

import java.util.List;

public class JacksonUtil {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 将对象转换成json字符串
     *
     * @param obj 对象
     * @return json字符串
     */
    @SneakyThrows
    public static String toJson(Object obj) {
        return MAPPER.writeValueAsString(obj);
    }

    /**
     * 将对象转换成带格式的json字符串
     */
    @SneakyThrows
    public static String toPrettyJson(Object obj) {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
    }

    /**
     * 将json字符串转换成对象
     *
     * @param json  json字符串
     * @param clazz 对象类型
     * @param <T>   对象类型
     * @return 对象
     */
    @SneakyThrows
    public static <T> T toObject(String json, Class<T> clazz) {
        return MAPPER.readValue(json, clazz);
    }
    /**
     * 将json字符串转换成List
     */
    @SneakyThrows
    public static <T> List<T> toList(String json, Class<T> clazz) {
        return MAPPER.readValue(json, MAPPER.getTypeFactory().constructCollectionType(List.class, clazz));
    }

    /**
     * 深拷贝
     */
    @SneakyThrows
    public static <T> T deepCopy(T obj, Class<T> clazz) {
        return toObject(toJson(obj), clazz);
    }
}
