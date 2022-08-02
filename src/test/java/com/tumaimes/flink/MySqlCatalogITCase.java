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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;

public class MySqlCatalogITCase extends MySqlCatalogTestBase {

    private TableEnvironment tEnv;

    @Before
    public void setup() {
        this.tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        // Use mysql catalog.
        tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
        tEnv.useCatalog(TEST_CATALOG_NAME);
    }

    // ------ databases ------

    @Test
    public void testGetDb_DatabaseNotExistException() throws Exception {
        String databaseNotExist = "nonexistent";
        assertThatThrownBy(() -> catalog.getDatabase(databaseNotExist))
                .satisfies(
                        anyCauseMatches(
                                DatabaseNotExistException.class,
                                String.format(
                                        "Database %s does not exist in Catalog",
                                        databaseNotExist)));
    }

    @Test
    public void testListDatabases() {
        List<String> actual = catalog.listDatabases();
        assertEquals(Collections.singletonList(TEST_DB), actual);
    }

    @Test
    public void testListFunctions() {
        List<String> listFunctions = catalog.listFunctions(TEST_DB);
        assertEquals(Collections.singletonList("splits"), listFunctions);
    }
}
