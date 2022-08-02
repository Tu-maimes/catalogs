package com.tumaimes.flink;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Tu_maimes
 * @description
 * @create 2022-07-13 15:13
 **/
public class TuMaimesCatalogTest {
    public static final Logger LOG = LoggerFactory.getLogger(TuMaimesCatalogTest.class);

    private TableEnvironment tEnv;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Before
    public void setUp() {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        tEnv = TableEnvironment.create(settings);
    }

    @Test
    public void testTableCatalog() throws Exception {
        tEnv.executeSql("CREATE CATALOG my_catalog WITH(\n" +
                "    'type' = 'tu_maimes',\n" +
                "    'default-database' = 'example_12',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root',\n" +
                "    'base-url' = 'jdbc:mysql://localhost:3306/catalog?useUnicode=true&characterEncoding=utf8'\n" +
                ");").print();
        tEnv.executeSql("use CATALOG my_catalog").print();
        tEnv.executeSql("show tables").print();

    }


    @Test
    public void testTableApiCatalog() throws Exception {
        String catalogName = "my_catalog";
        String defaultDatabase = "example_12";
        String username = "root";
        String password = "root";
        String jdbcUrl = "jdbc:mysql://localhost:3306/catalog?useUnicode=true&characterEncoding=utf8";
        TuMaimesCatalog mysqlCatalog = new TuMaimesCatalog(catalogName, defaultDatabase, username, password, jdbcUrl);
        tEnv.registerCatalog(catalogName, mysqlCatalog);
        tEnv.useCatalog(catalogName);
        tEnv.executeSql("use CATALOG my_catalog").print();
        tEnv.executeSql("show tables").print();

    }

    @Test
    public void testFunctionApiCatalog() throws Exception {
        String catalogName = "my_catalog";
        String defaultDatabase = "example_12";
        String username = "root";
        String password = "root";
        String jdbcUrl = "jdbc:mysql://localhost:3306/catalog?useUnicode=true&characterEncoding=utf8";
        TuMaimesCatalog mysqlCatalog = new TuMaimesCatalog(catalogName, defaultDatabase, username, password, jdbcUrl);
        tEnv.registerCatalog(catalogName, mysqlCatalog);
        tEnv.useCatalog(catalogName);
        tEnv.executeSql("show functions").print();

    }
}
