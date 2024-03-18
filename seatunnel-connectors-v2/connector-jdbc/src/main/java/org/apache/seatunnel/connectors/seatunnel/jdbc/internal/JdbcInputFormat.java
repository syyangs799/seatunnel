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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.CopyManagerProxy;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.ChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * InputFormat to read data from a database and generate Rows. The InputFormat has to be configured
 * using the supplied InputFormatBuilder. A valid RowTypeInfo must be properly configured in the
 * builder
 */
public class JdbcInputFormat implements Serializable {

    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcInputFormat.class);

    private final JdbcDialect jdbcDialect;
    private final JdbcRowConverter jdbcRowConverter;
    private final Map<TablePath, CatalogTable> tables;
    private final ChunkSplitter chunkSplitter;

    private transient String splitTableId;
    private transient TableSchema splitTableSchema;
    private transient PreparedStatement statement;
    private transient String statementSql;
    private transient ResultSet resultSet;
    private volatile boolean hasNext;
    private volatile boolean useCopyStatement;
    CopyManagerProxy copyManagerProxy;

    private static final int BUFFER_SIZE = 4096; // 设置缓存大小

    public JdbcInputFormat(JdbcSourceConfig config, Map<TablePath, CatalogTable> tables) {
        this.jdbcDialect =
                JdbcDialectLoader.load(
                        config.getJdbcConnectionConfig().getUrl(), config.getCompatibleMode());
        this.chunkSplitter = ChunkSplitter.create(config);
        this.jdbcRowConverter = jdbcDialect.getRowConverter();
        this.tables = tables;
        // 此处判断只有postgres可以进行copy
        this.useCopyStatement =
                config.isUseCopyStatement() && (this.jdbcDialect instanceof PostgresDialect);
    }

    public void openInputFormat() {}

    public void closeInputFormat() throws IOException {
        close();

        if (chunkSplitter != null) {
            chunkSplitter.close();
        }
    }

    /**
     * Connects to the source database and executes the query
     *
     * @param inputSplit which is ignored if this InputFormat is executed as a non-parallel source,
     *     a "hook" to the query parameters otherwise (using its <i>parameterId</i>)
     * @throws IOException if there's an error during the execution of the query
     */
    public void open(JdbcSourceSplit inputSplit) throws IOException {
        try {
            splitTableSchema = tables.get(inputSplit.getTablePath()).getTableSchema();
            splitTableId = inputSplit.getTablePath().toString();
            if (useCopyStatement) {
                LOG.info(
                        "当前数据源类型【{}】,并且配置useCopyStatement【{}】，成功开启COPY模式读取数据!!!",
                        this.jdbcDialect.dialectName(),
                        true);
                // 判断如果当前类型为postgres并且开启copy参数，则使用最新的copy代码将数据导出
                statementSql = chunkSplitter.generateCopySplitStatementSql(inputSplit);
                this.copyManagerProxy =
                        new CopyManagerProxy(chunkSplitter.getOrEstablishConnection());
            } else {
                statement = chunkSplitter.generateSplitStatement(inputSplit);

                resultSet = statement.executeQuery();
                hasNext = resultSet.next();
            }
        } catch (SQLException se) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                    "open() failed." + se.getMessage(),
                    se);
        } catch (Exception e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED, e.getMessage());
        }
    }

    /**
     * Closes all resources used.
     *
     * @throws IOException Indicates that a resource could not be closed.
     */
    public void close() throws IOException {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOG.info("ResultSet couldn't be closed - " + e.getMessage());
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.info("Statement couldn't be closed - " + e.getMessage());
            }
        }
    }

    /**
     * Checks whether all data has been read.
     *
     * @return boolean value indication whether all data has been read.
     */
    public boolean reachedEnd() {
        return !hasNext;
    }

    public boolean copyEnable() {
        return useCopyStatement;
    }

    /** Convert a row of data to seatunnelRow */
    public SeaTunnelRow nextRecord() {
        try {
            if (!hasNext) {
                return null;
            }
            SeaTunnelRow seaTunnelRow = jdbcRowConverter.toInternal(resultSet, splitTableSchema);
            seaTunnelRow.setTableId(splitTableId);
            seaTunnelRow.setRowKind(RowKind.INSERT);

            // update hasNext after we've read the record
            hasNext = resultSet.next();
            return seaTunnelRow;
        } catch (SQLException se) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "Couldn't read data - " + se.getMessage(),
                    se);
        } catch (NullPointerException npe) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "Couldn't access resultSet",
                    npe);
        }
    }

    /** Convert a row of data to seatunnelRow */
    public void copyAllRecord(Collector<SeaTunnelRow> collector) {

        try {
            String copySql = "COPY (" + statementSql + ") TO STDOUT DELIMITER '|' CSV";
            // Execute COPY TO STDOUT command
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            long bytesCopied = this.copyManagerProxy.doCopyOut(copySql, byteArrayOutputStream);
            // Parse and process copied data
            // Get the byte array written to ByteArrayOutputStream
            byte[] data = byteArrayOutputStream.toByteArray();
            // Process data in chunks
            int offset = 0;
            StringBuilder lineBuffer = new StringBuilder();
            while (offset < data.length) {
                // Calculate the remaining bytes to be read in this chunk
                int remaining = Math.min(BUFFER_SIZE, data.length - offset);
                // Read the chunk of data into a byte array
                byte[] buffer = new byte[remaining];
                System.arraycopy(data, offset, buffer, 0, remaining);
                // Process the chunk of data
                offset += remaining;
                lineBuffer.append(new String(buffer));
                String[] lines = lineBuffer.toString().split("\\r?\\n");
                if (lines.length > 1) {
                    // 不截断最后一行，否则数据出错
                    for (int i = 0; i < lines.length - 1; i++) {
                        // Process each complete line of data as needed=
                        String[] fields =
                                lines[i].split("\\|"); // Assuming '|' is used as delimiter
                        SeaTunnelRow seaTunnelRow =
                                jdbcRowConverter.toInternal(fields, splitTableSchema);
                        seaTunnelRow.setTableId(splitTableId);
                        seaTunnelRow.setRowKind(RowKind.INSERT);
                        collector.collect(seaTunnelRow);
                    }
                    // Save the last potentially incomplete line for the next iteration
                    lineBuffer.setLength(0);
                    lineBuffer.append(lines[lines.length - 1]);
                }
            }
            LOG.info("Total bytes copied: " + bytesCopied);

        } catch (SQLException se) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "Couldn't read data - " + se.getMessage(),
                    se);
        } catch (NullPointerException npe) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "Couldn't access resultSet",
                    npe);
        } catch (Exception npe) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "Couldn't access resultSet",
                    npe);
        }
    }
}
