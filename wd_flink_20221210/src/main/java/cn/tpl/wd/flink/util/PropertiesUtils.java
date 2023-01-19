package cn.tpl.wd.flink.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @author ChenXin
 * @create 2022-12-10-19:56
 */
public class PropertiesUtils {
    /**
     * 获取的是class的根路径下的文件
     * 优点是：可以在非Web应用中读取配置资源信息，可以读取任意的资源文件信息
     * 缺点：只能加载类classes下面的资源文件。
     */
    public static Properties getProperties() {
        //文件在class的根路径
        InputStream is = PropertiesUtils.class.getClassLoader().getResourceAsStream("application.properties");

        //获取文件的位置

        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        Properties props = new Properties();

        try {
            props.load(br);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }


    public static void main(String[] args) {
        Properties properties = PropertiesUtils.getProperties();
        System.out.println(properties.get("es.port"));
        System.out.println(properties.get("es.username"));
        System.out.println(properties.get("es.hostname"));
        System.out.println(properties.get("es.password"));

    }


}
