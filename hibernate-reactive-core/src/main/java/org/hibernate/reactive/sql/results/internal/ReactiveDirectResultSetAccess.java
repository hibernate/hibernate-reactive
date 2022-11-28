/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.sql.results.jdbc.internal.DirectResultSetAccess;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMetadata;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

public class ReactiveDirectResultSetAccess extends DirectResultSetAccess implements ReactiveResultSetAccess {

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
	public SessionFactoryImplementor getFactory() {
		return getPersistenceContext().getFactory();
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
	public CompletionStage<ResultSet> getReactiveResultSet() {
		return completedFuture( resultSet );
	}

	@Override
	public CompletionStage<ResultSetMetaData> getReactiveMetadata() {
		return completedFuture( getMetaData() );
	}

	@Override
	public CompletionStage<Integer> getReactiveColumnCount() {
		return completedFuture( getColumnCount() );
	}

	@Override
	public CompletionStage<JdbcValuesMetadata> resolveJdbcValueMetadata() {
		return null;
	}
}
