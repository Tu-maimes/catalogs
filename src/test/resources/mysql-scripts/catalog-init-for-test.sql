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

/**
* The test for mysql 5.7.X & 8.0.X versions.
* The init script contains some types that are incompatible with 5.6.X or lower versions.
*/

-- Creates test user info and grants privileges.
CREATE USER 'mysql'@'%' IDENTIFIED BY 'mysql';
GRANT ALL ON *.* TO 'mysql'@'%';
FLUSH PRIVILEGES;

-- Create the `test` database.
DROP DATABASE IF EXISTS `test`;
CREATE DATABASE `test` CHARSET=utf8;

-- Uses `test` database.
use `test`;

-- Create test tables.
-- ----------------------------
-- Table structure for t_all_types
-- ----------------------------
DROP TABLE IF EXISTS `catalogs`;
CREATE TABLE `catalogs` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `database_name` varchar(100) NOT NULL,
  `table_name` varchar(100) NOT NULL,
  `table_params` text NOT NULL,
  `create_times` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of catalogs
-- ----------------------------
INSERT INTO `catalogs` VALUES ('1', 'test', 'T_ODS_TRADE_STOCK', '{\"schema.7.data-type\":\"DECIMAL(16, 2)\",\"schema.13.data-type\":\"VARCHAR(2147483647)\",\"schema.0.data-type\":\"BIGINT NOT NULL\",\"schema.11.data-type\":\"DECIMAL(16, 2)\",\"schema.9.name\":\"TODAY_ENTRUST_AMOUNT\",\"schema.11.name\":\"TODAY_DEAL_AMOUNT\",\"schema.1.name\":\"TRADE_DATE\",\"schema.14.name\":\"TRADE_TYPE\",\"schema.4.name\":\"SEC_CD\",\"schema.3.data-type\":\"VARCHAR(2147483647)\",\"schema.12.name\":\"TODAY_DEAL_BALANCE\",\"schema.3.name\":\"PF_ID\",\"schema.7.name\":\"BALANCE\",\"schema.6.name\":\"AMOUNT\",\"schema.5.data-type\":\"VARCHAR(2147483647)\",\"schema.14.data-type\":\"VARCHAR(2147483647)\",\"schema.0.name\":\"SERIAL_NO\",\"schema.5.name\":\"ENTRUST_DIRECTION\",\"schema.12.data-type\":\"DECIMAL(16, 2)\",\"schema.8.data-type\":\"DECIMAL(16, 2)\",\"schema.2.name\":\"INVEST_TYPE\",\"format\":\"ogg-json\",\"schema.6.data-type\":\"DECIMAL(16, 2)\",\"schema.1.data-type\":\"INT\",\"properties.bootstrap.servers\":\"localhost:30802\",\"schema.2.data-type\":\"VARCHAR(2147483647)\",\"schema.10.data-type\":\"DECIMAL(16, 2)\",\"connector\":\"kafka\",\"schema.primary-key.name\":\"PK_-2081830901\",\"schema.primary-key.columns\":\"SERIAL_NO\",\"topic\":\"T_ODS_TRADE_STOCK_TOPIC_MOCK\",\"properties.group.id\":\"testGroup\",\"schema.9.data-type\":\"DECIMAL(16, 2)\",\"schema.13.name\":\"TRADE_STATUS\",\"comment\":\"交易-快照信息\",\"schema.4.data-type\":\"VARCHAR(2147483647)\",\"schema.10.name\":\"TODAY_ENTRUST_BALANCE\",\"schema.8.name\":\"FACT_PRICE\"}', '2022-07-29 11:16:52');

DROP TABLE IF EXISTS `functions`;
CREATE TABLE `functions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `db_name` varchar(50) NOT NULL,
  `fun_name` varchar(50) NOT NULL,
  `fqn` varchar(50) NOT NULL,
  `function_language` varchar(50) NOT NULL,
  `create_time` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of functions
-- ----------------------------
INSERT INTO `functions` VALUES ('1', 'test', 'splits', 'com.apache.catalog.splits', 'JAVA', '2022-07-29 12:45:44');
