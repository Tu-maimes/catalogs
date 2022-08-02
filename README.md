## 1、简介

**TuMaimesCatalog** 是Flink生态下的插件扩展，主要是为在不使用Hive  Metastore 时利用Mysql来存储Flink的DDL语句与自定义Function函数，可以做到持久化存储是对默认内存存储的功能补充。

## 2、编译

```shell
mvn clean install  -DskipTests 
```

## 3、Sql 脚本

```sql
   -- 函数表元数据表
        CREATE TABLE `functions` (
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `db_name` varchar(50) NOT NULL,
        `fun_name` varchar(50) NOT NULL,
        `fqn` varchar(50) NOT NULL,
        `function_language` varchar(50) NOT NULL,
        `create_time` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`)
        ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

       -- Flink表元数据表
        CREATE TABLE `catalogs` (
         `id` int(10) NOT NULL AUTO_INCREMENT,
         `database_name` varchar(100) NOT NULL,
         `table_name` varchar(100) NOT NULL,
         `table_params` text NOT NULL,
         `create_times` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP,
         PRIMARY KEY (`id`)
         ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
```

初始化到Mysql数据库catalog。

### 4、示例

### 4.1、Table API

```java
 EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        tEnv = TableEnvironment.create(settings);
        
  tEnv.executeSql("CREATE CATALOG my_catalog WITH(\n" +
                "    'type' = 'tu_maimes',\n" +
                "    'default-database' = 'example_12',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root',\n" +
                "    'base-url' = 'jdbc:mysql://localhost:3306/catalog?useUnicode=true&characterEncoding=utf8'\n" +
                ");").print();
        tEnv.executeSql("use CATALOG my_catalog").print();
        tEnv.executeSql("show tables").print();
```

### 4.2、DataStream API

```java
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
```

