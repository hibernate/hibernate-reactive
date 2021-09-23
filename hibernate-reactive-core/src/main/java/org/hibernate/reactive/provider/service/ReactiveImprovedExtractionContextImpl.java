/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.resource.transaction.spi.DdlTransactionIsolator;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tool.schema.internal.exec.ImprovedExtractionContextImpl;
import org.hibernate.tool.schema.internal.exec.JdbcContext;

import static org.hibernate.reactive.util.impl.CompletionStages.logSqlException;
import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;

public class ReactiveImprovedExtractionContextImpl extends ImprovedExtractionContextImpl {

	private final ReactiveConnectionPool service;

	public ReactiveImprovedExtractionContextImpl(
			ServiceRegistry registry,
			Identifier defaultCatalog,
			Identifier defaultSchema,
			DatabaseObjectAccess databaseObjectAccess) {
		super(
				registry,
				registry.getService( JdbcEnvironment.class ),
				new DdlTransactionIsolator() {
					@Override
					public JdbcContext getJdbcContext() {
						return null;
					}

					@Override
					public void prepare() {
					}

					@Override
					public Connection getIsolatedConnection() {
						return new Connection() {
							@Override
							public Statement createStatement() throws SQLException {
								return null;
							}

							@Override
							public PreparedStatement prepareStatement(String sql) throws SQLException {
								return null;
							}

							@Override
							public CallableStatement prepareCall(String sql) throws SQLException {
								return null;
							}

							@Override
							public String nativeSQL(String sql) throws SQLException {
								return null;
							}

							@Override
							public void setAutoCommit(boolean autoCommit) throws SQLException {

							}

							@Override
							public boolean getAutoCommit() throws SQLException {
								return false;
							}

							@Override
							public void commit() throws SQLException {

							}

							@Override
							public void rollback() throws SQLException {

							}

							@Override
							public void close() throws SQLException {

							}

							@Override
							public boolean isClosed() throws SQLException {
								return false;
							}

							@Override
							public DatabaseMetaData getMetaData() throws SQLException {
								return null;
							}

							@Override
							public void setReadOnly(boolean readOnly) throws SQLException {

							}

							@Override
							public boolean isReadOnly() throws SQLException {
								return false;
							}

							@Override
							public void setCatalog(String catalog) throws SQLException {

							}

							@Override
							public String getCatalog() throws SQLException {
								return null;
							}

							@Override
							public void setTransactionIsolation(int level) throws SQLException {

							}

							@Override
							public int getTransactionIsolation() throws SQLException {
								return 0;
							}

							@Override
							public SQLWarning getWarnings() throws SQLException {
								return null;
							}

							@Override
							public void clearWarnings() throws SQLException {

							}

							@Override
							public Statement createStatement(int resultSetType, int resultSetConcurrency)
									throws SQLException {
								return null;
							}

							@Override
							public PreparedStatement prepareStatement(
									String sql,
									int resultSetType,
									int resultSetConcurrency) throws SQLException {
								return null;
							}

							@Override
							public CallableStatement prepareCall(
									String sql,
									int resultSetType,
									int resultSetConcurrency) throws SQLException {
								return null;
							}

							@Override
							public Map<String, Class<?>> getTypeMap() throws SQLException {
								return null;
							}

							@Override
							public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

							}

							@Override
							public void setHoldability(int holdability) throws SQLException {

							}

							@Override
							public int getHoldability() throws SQLException {
								return 0;
							}

							@Override
							public Savepoint setSavepoint() throws SQLException {
								return null;
							}

							@Override
							public Savepoint setSavepoint(String name) throws SQLException {
								return null;
							}

							@Override
							public void rollback(Savepoint savepoint) throws SQLException {

							}

							@Override
							public void releaseSavepoint(Savepoint savepoint) throws SQLException {

							}

							@Override
							public Statement createStatement(
									int resultSetType,
									int resultSetConcurrency,
									int resultSetHoldability) throws SQLException {
								return null;
							}

							@Override
							public PreparedStatement prepareStatement(
									String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
									throws SQLException {
								return null;
							}

							@Override
							public CallableStatement prepareCall(
									String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
									throws SQLException {
								return null;
							}

							@Override
							public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
									throws SQLException {
								return null;
							}

							@Override
							public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
									throws SQLException {
								return null;
							}

							@Override
							public PreparedStatement prepareStatement(String sql, String[] columnNames)
									throws SQLException {
								return null;
							}

							@Override
							public Clob createClob() throws SQLException {
								return null;
							}

							@Override
							public Blob createBlob() throws SQLException {
								return null;
							}

							@Override
							public NClob createNClob() throws SQLException {
								return null;
							}

							@Override
							public SQLXML createSQLXML() throws SQLException {
								return null;
							}

							@Override
							public boolean isValid(int timeout) throws SQLException {
								return false;
							}

							@Override
							public void setClientInfo(String name, String value) throws SQLClientInfoException {

							}

							@Override
							public void setClientInfo(Properties properties) throws SQLClientInfoException {

							}

							@Override
							public String getClientInfo(String name) throws SQLException {
								return null;
							}

							@Override
							public Properties getClientInfo() throws SQLException {
								return null;
							}

							@Override
							public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
								return null;
							}

							@Override
							public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
								return null;
							}

							@Override
							public void setSchema(String schema) throws SQLException {

							}

							@Override
							public String getSchema() throws SQLException {
								return null;
							}

							@Override
							public void abort(Executor executor) throws SQLException {

							}

							@Override
							public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

							}

							@Override
							public int getNetworkTimeout() throws SQLException {
								return 0;
							}

							@Override
							public <T> T unwrap(Class<T> iface) throws SQLException {
								return null;
							}

							@Override
							public boolean isWrapperFor(Class<?> iface) throws SQLException {
								return false;
							}
						};
					}

					@Override
					public void release() {
					}
				},
				defaultCatalog,
				defaultSchema,
				databaseObjectAccess
		);
		service = registry.getService( ReactiveConnectionPool.class );
	}

	@Override
	public <T> T getQueryResults(
			String queryString,
			Object[] positionalParameters,
			ResultSetProcessor<T> resultSetProcessor) throws SQLException {

		final CompletionStage<ReactiveConnection> connectionStage = service.getConnection();

		try (final ResultSet resultSet = getQueryResultSet( queryString, positionalParameters, connectionStage )) {
			return resultSetProcessor.process( resultSet );
		}
		finally {
			// We start closing the connection but we don't care about the result
			connectionStage.whenComplete( (c, e) -> c.close() );
		}
	}

	private ResultSet getQueryResultSet(
			String queryString,
			Object[] positionalParameters,
			CompletionStage<ReactiveConnection> connectionStage) {
		final Object[] parametersToUse = positionalParameters != null ? positionalParameters : new Object[0];
		final Parameters parametersDialectSpecific = Parameters.instance(
				getJdbcEnvironment().getDialect()
		);
		final String queryToUse = parametersDialectSpecific.process( queryString, parametersToUse.length );
		return connectionStage.thenCompose( c -> c.selectJdbcOutsideTransaction( queryToUse, parametersToUse ) )
				.handle( (resultSet, err) -> {
					logSqlException( err, () -> "could not execute query ", queryToUse );
					return returnOrRethrow( err, resultSet );
				} )
				.toCompletableFuture()
				.join();
	}
}
