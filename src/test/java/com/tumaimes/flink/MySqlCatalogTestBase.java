/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tumaimes.flink;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

public class MySqlCatalogTestBase {

    public static final Logger LOG = LoggerFactory.getLogger(MySqlCatalogTestBase.class);

    protected static final DockerImageName MYSQL_57_IMAGE = DockerImageName.parse("mysql:5.7.34");
    protected static final String TEST_CATALOG_NAME = "mysql_catalog";
    protected static final String TEST_USERNAME = "mysql";
    protected static final String TEST_PWD = "mysql";
    protected static final String TEST_DB = "test";
    protected static final String MYSQL_INIT_SCRIPT = "mysql-scripts/catalog-init-for-test.sql";
    protected static final Map<String, String> DEFAULT_CONTAINER_ENV_MAP =
            new HashMap<String, String>() {
                {
                    put("MYSQL_ROOT_HOST", "%");
                }
            };
    @ClassRule
    public static final MySQLContainer<?> MYSQL_CONTAINER =
            new MySQLContainer<>(MYSQL_57_IMAGE)
                    .withUsername("root")
                    .withPassword("")
                    .withEnv(DEFAULT_CONTAINER_ENV_MAP)
                    .withInitScript(MYSQL_INIT_SCRIPT)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected static TuMaimesCatalog catalog;

    @BeforeClass
    public static void beforeAll() {
        catalog = new TuMaimesCatalog(TEST_CATALOG_NAME, TEST_DB, TEST_USERNAME, TEST_PWD,
                MYSQL_CONTAINER.getJdbcUrl());
    }
}
