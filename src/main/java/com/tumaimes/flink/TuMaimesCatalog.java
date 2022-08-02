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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/*

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

*/


/**
 * @author Tu_maimes
 * &#064;description  Catalog for MySQL.
 * &#064;create  2022-07-28 15:16
 **/
public class TuMaimesCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(TuMaimesCatalog.class);
    private static final String SELECT_DB = "SELECT database_name from catalogs WHERE database_name = ?";
    private static final String SHOW_DATABASES = "SELECT DISTINCT database_name  from catalogs";
    private static final String SHOW_TABLES = "SELECT table_name  from catalogs WHERE database_name = ?";
    private static final String DELETE_TABLE = "DELETE FROM catalogs WHERE database_name = ? AND table_name = ?";

    private static final String SELECT_TABLE = "SELECT * from catalogs WHERE database_name = ? AND table_name = ?";
    private static final String CREATE_TABLE = "INSERT INTO `catalogs` (`database_name`, `table_name`, `table_params`,`create_times`) VALUES (?, ?, ?,SYSDATE())";

    private static final String CREATE_FUN = "INSERT INTO `functions` (`db_name`, `fun_name`, `fqn`, `function_language`, `create_time`) VALUES (?, ?, ?, ?, SYSDATE())";
    private static final String SHOW_FUN = "SELECT fun_name from functions";
    private static final String SELECT_FUN = "SELECT fqn,function_language from functions WHERE db_name = ? and fun_name = ?";

    private static final String DELETE_FUN = "DELETE FROM functions WHERE db_name = ? and fun_name = ?";
    protected final String username;
    protected final String pwd;
    protected final String baseUrl;
    protected final String defaultUrl;

    public TuMaimesCatalog(String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
        super(catalogName, defaultDatabase);
        this.username = username;
        this.pwd = pwd;
        this.baseUrl = baseUrl;
        this.defaultUrl = baseUrl;
    }

    @Override
    public void open() throws CatalogException {

    }

    @Override
    public void close() throws CatalogException {

    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            ResultSet resultSet = conn.prepareStatement(SHOW_DATABASES).executeQuery();
            List<String> databases = Lists.newArrayList();
            while (resultSet.next()) {
                databases.add(resultSet.getString(1));
            }
            return databases;
        } catch (SQLException e) {
            throw new CatalogException(e.getMessage(), e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws CatalogException, DatabaseNotExistException {
        if (listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            PreparedStatement preparedStatement = conn.prepareStatement(SELECT_DB);
            preparedStatement.setString(1, databaseName);
            preparedStatement.executeQuery();
            return preparedStatement.getResultSet().next();
        } catch (SQLException e) {
            throw new CatalogException(e.getMessage(), e);
        }
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listTables(String databaseName) throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            PreparedStatement preparedStatement = conn.prepareStatement(SHOW_TABLES);
            preparedStatement.setString(1, databaseName);
            preparedStatement.executeQuery();
            ResultSet resultSet = preparedStatement.executeQuery();
            List<String> tables = Lists.newArrayList();
            while (resultSet.next()) {
                tables.add(resultSet.getString(1));
            }
            return tables;
        } catch (SQLException e) {
            throw new CatalogException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> listViews(String databaseName) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            PreparedStatement selectTable = conn.prepareStatement(SELECT_TABLE);
            selectTable.setString(1, tablePath.getDatabaseName());
            selectTable.setString(2, tablePath.getObjectName());
            ResultSet resultSet = selectTable.executeQuery();
            return resultSet.next();
        } catch (SQLException e) {
            throw new ValidationException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            PreparedStatement selectTable = conn.prepareStatement(DELETE_TABLE);
            selectTable.setString(1, tablePath.getDatabaseName());
            selectTable.setString(2, tablePath.getObjectName());
            selectTable.execute();
        } catch (SQLException e) {
            throw new ValidationException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, CatalogException {

        checkNotNull(tablePath, "tablePath cannot be null");
        checkNotNull(table, "table cannot be null");
        if (tableExists(tablePath)) {
            throw new TableAlreadyExistException(getName(), tablePath);
        }
        String databaseName = tablePath.getDatabaseName();
        String tableName = tablePath.getObjectName();
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            Map<String, String> catalogTable = CatalogPropertiesUtil.serializeCatalogTable((ResolvedCatalogTable) table);
            PreparedStatement createTable = conn.prepareStatement(CREATE_TABLE);
            createTable.setString(1, databaseName);
            createTable.setString(2, tableName);
            ObjectMapper mapper = new ObjectMapper();
            createTable.setString(3, mapper.writeValueAsString(catalogTable));
            createTable.execute();
        } catch (SQLException e) {
            throw new ValidationException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to serialize catalog table", e);
            throw new RuntimeException(e);
        }

    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listFunctions(String dbName) throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            ResultSet resultSet = conn.prepareStatement(SHOW_FUN).executeQuery();
            List<String> functions = Lists.newArrayList();
            while (resultSet.next()) {
                functions.add(resultSet.getString(1));
            }
            return functions;
        } catch (SQLException e) {
            throw new ValidationException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        }
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {

        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            PreparedStatement preparedStatement = conn.prepareStatement(SELECT_FUN);
            preparedStatement.setString(1, functionPath.getDatabaseName());
            preparedStatement.setString(2, functionPath.getObjectName());
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String fqn = resultSet.getString("fqn");
                String language = resultSet.getString("function_language");
                FunctionLanguage functionLanguage = FunctionLanguage.valueOf(language.toUpperCase());
                return new CatalogFunctionImpl(fqn, functionLanguage);
            } else {
                throw new FunctionNotExistException(getName(), functionPath);
            }
        } catch (SQLException e) {
            throw new ValidationException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        }
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            PreparedStatement preparedStatement = conn.prepareStatement(SELECT_FUN);
            preparedStatement.setString(1, functionPath.getDatabaseName());
            preparedStatement.setString(2, functionPath.getObjectName());
            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet.next();
        } catch (SQLException e) {
            throw new ValidationException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        }
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, CatalogException {
        if (functionExists(functionPath)) {
            throw new FunctionAlreadyExistException(getName(), functionPath);
        }
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            PreparedStatement createTable = conn.prepareStatement(CREATE_FUN);
            createTable.setString(1, functionPath.getDatabaseName());
            createTable.setString(2, functionPath.getObjectName());
            createTable.setString(3, function.getClassName());
            createTable.setString(4, function.getFunctionLanguage().name());
            createTable.execute();
        } catch (SQLException e) {
            throw new ValidationException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        }
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            PreparedStatement createTable = conn.prepareStatement(DELETE_FUN);
            createTable.setString(1, functionPath.getDatabaseName());
            createTable.setString(2, functionPath.getObjectName());
            createTable.execute();
        } catch (SQLException e) {
            throw new ValidationException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        }
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws CatalogException, TableNotExistException {
        String tableName = tablePath.getObjectName();
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            PreparedStatement statement = conn.prepareStatement(SELECT_TABLE);
            statement.setString(1, tablePath.getDatabaseName());
            statement.setString(2, tableName);
            ResultSet rs = statement.executeQuery();
            if (!rs.next()) {
                throw new TableNotExistException(getName(), tablePath);
            }
            String table_params = rs.getString("table_params");
            Map<String, String> properties = new ObjectMapper().readValue(table_params, new TypeReference<Map<String, String>>() {
            });
            return CatalogPropertiesUtil.deserializeCatalogTable(properties);
        } catch (SQLException e) {
            throw new ValidationException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to deserialize table properties", e);
            throw new RuntimeException(e);
        }
    }
}
