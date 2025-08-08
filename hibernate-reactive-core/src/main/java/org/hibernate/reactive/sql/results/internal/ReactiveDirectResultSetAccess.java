/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletionStage;

import org.hibernate.JDBCException;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.sql.results.jdbc.internal.DirectResultSetAccess;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMetadata;
import org.hibernate.type.BasicType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.spi.TypeConfiguration;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

public class ReactiveDirectResultSetAccess extends DirectResultSetAccess implements ReactiveResultSetAccess {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private PreparedStatement resultSetSource;
	private ResultSet resultSet;

	public ReactiveDirectResultSetAccess(
			SharedSessionContractImplementor persistenceContext,
			PreparedStatement resultSetSource,
			ResultSet resultSet) {
		super( persistenceContext, resultSetSource, resultSet );
		this.resultSetSource = resultSetSource;
		this.resultSet = resultSet;
	}

	@Override
	public JdbcServices getJdbcServices() {
		return getFactory().getJdbcServices();
	}

	@Override
	public JDBCException convertSqlException(SQLException e, String message) {
		return getJdbcServices().getJdbcEnvironment().getSqlExceptionHelper().convert( e, message );
	}

	@Override
	public void release() {
		// Not sure if this is needed for reactive
		getPersistenceContext().getJdbcCoordinator()
				.getLogicalConnection()
				.getResourceRegistry()
				.release( resultSet, resultSetSource );
	}

	@Override
	public int getResultCountEstimate() {
		return super.getResultCountEstimate();
	}

	@Override
	public <J> BasicType<J> resolveType(int position, JavaType<J> explicitJavaType, SessionFactoryImplementor sessionFactory) {
		return super.resolveType( position, explicitJavaType, sessionFactory );
	}

	@Override
	public <J> BasicType<J> resolveType(int position, JavaType<J> explicitJavaType, TypeConfiguration typeConfiguration) {
		return super.resolveType( position, explicitJavaType, typeConfiguration );
	}

	@Override
	public CompletionStage<ResultSet> getReactiveResultSet() {
		return completedFuture( resultSet );
	}

	@Override
	public CompletionStage<Integer> getReactiveColumnCount() {
		return completedFuture( getColumnCount() );
	}

	@Override
	public CompletionStage<JdbcValuesMetadata> resolveJdbcValueMetadata() {
		throw LOG.notYetImplemented();
	}

	@Override
	public ResultSet getResultSet() {
		return resultSet;
	}
}
