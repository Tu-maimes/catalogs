package com.tumaimes.flink;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import static com.tumaimes.flink.MySqlCatalogTestBase.*;

/**
 * @author Tu_maimes
 * @description
 * @create 2022-08-01 10:14
 **/
public class T {
    public static void main(String[] args) {
       MySQLContainer<?> MYSQL_CONTAINER =
                new MySQLContainer<>(MYSQL_57_IMAGE)
                        .withUsername("root")
                        .withPassword("")
                        .withEnv(DEFAULT_CONTAINER_ENV_MAP)
                        .withInitScript(MYSQL_INIT_SCRIPT)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        MYSQL_CONTAINER.start();
        System.out.println(MYSQL_CONTAINER.getJdbcUrl());
    }
}
