package cn.voriya.kafka.metrics.config;

import cn.voriya.kafka.metrics.utils.JacksonUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Log4j2
public class Config {
    private static Config instance;

    public static synchronized Config getInstance() {
        return instance;
    }

    public static synchronized void updateOrInsertCluster(ConfigCluster cluster) {
        Config newInstance = JacksonUtil.deepCopy(instance, Config.class);
        newInstance.getCluster().stream().filter(c -> c.getName().equals(cluster.getName())).findFirst().ifPresentOrElse(
                c -> {
                    newInstance.getCluster().remove(c);
                    newInstance.getCluster().add(cluster);
                },
                () -> newInstance.getCluster().add(cluster)
        );
        instance = newInstance;
    }

    public static synchronized void removeCluster(String clusterName) {
        Config newInstance = JacksonUtil.deepCopy(instance, Config.class);
        newInstance.getCluster().removeIf(c -> c.getName().equals(clusterName));
        instance = newInstance;
    }

    public static String getDefaultConfigPath() {
        try {
            //获取jar包所在目录
            String jarPath = new File(Config.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
            //移除jar包名
            return jarPath.substring(0, jarPath.lastIndexOf(File.separator) + 1);
        } catch (URISyntaxException e) {
            log.error("get default config path error", e);
            return null;
        }
    }

    public static void parseConfigFromYaml(String path) {
        //解析配置文件
        Yaml yaml = new Yaml();
        File file = new File(path);
        try {
            instance = yaml.loadAs(file.toURI().toURL().openStream(), Config.class);
        } catch (Exception e) {
            log.error("parse config error", e);
        }
    }

    public static void parseConfigFromJson(String path) {
        //解析配置文件
        try {
            String jsonString = Files.readString(new File(path).toPath());
            instance = JacksonUtil.toObject(jsonString, Config.class);
        } catch (Exception e) {
            log.error("parse config error", e);
        }
    }

    public static void parseConfig() {
        String yamlFilePath = getDefaultConfigPath() + "conf.yaml";
        String jsonFilePath = getDefaultConfigPath() + "conf.json";
        if (new File(yamlFilePath).exists()) {
            log.info("parse config from yaml file: {}", yamlFilePath);
            parseConfigFromYaml(yamlFilePath);
        } else if (new File(jsonFilePath).exists()) {
            log.info("parse config from json file: {}", jsonFilePath);
            parseConfigFromJson(jsonFilePath);
        } else {
            log.warn("config file not found");
        }
    }
    private List<ConfigCluster> cluster;
    private Integer port = 4399;
    private Integer interval = 60;
}
