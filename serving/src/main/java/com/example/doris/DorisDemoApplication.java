package com.example.doris;

// Spring Boot 的入口注解：包含 @Configuration、@EnableAutoConfiguration、@ComponentScan
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
// 把 @ConfigurationProperties 标注的类注册成 Bean，这样 DorisProperties 才能被注入
import org.springframework.boot.context.properties.EnableConfigurationProperties;

// 导入我们自己的配置属性类
import com.example.doris.config.DorisProperties;

/**
 * 整个服务的启动类。
 *
 * <p>启动后会自动扫描 com.example.doris 包下的 @Component、@Service、@RestController 等。</p>
 */
@SpringBootApplication
@EnableConfigurationProperties(DorisProperties.class)
public class DorisDemoApplication {

    /**
     * main 方法：Spring Boot 应用的标准入口。
     *
     * @param args 命令行参数，例如 --server.port=9090
     */
    public static void main(String[] args) {
        // 启动内嵌 Tomcat、创建 Spring 容器、加载配置、初始化连接池
        SpringApplication.run(DorisDemoApplication.class, args);
    }
}
