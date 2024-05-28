package cn.voriya.kafka.metrics.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Log4j2
public class Config {
    @Getter
    private static Config instance;

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

    public static void parseConfig(String path) {
        //解析配置文件
        Yaml yaml = new Yaml();
        File file = new File(path);
        try {
            instance = yaml.loadAs(file.toURI().toURL().openStream(), Config.class);
        } catch (Exception e) {
            log.error("parse config error", e);
        }
    }
    private List<ConfigCluster> cluster;
    private String port = "1234";
}
