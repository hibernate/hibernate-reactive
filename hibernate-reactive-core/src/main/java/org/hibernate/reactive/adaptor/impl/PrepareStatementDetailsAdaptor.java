/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.adaptor.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.jdbc.Expectation;
import org.hibernate.sql.model.TableMapping;

public class PrepareStatementDetailsAdaptor implements PreparedStatementDetails {

	private final PreparedStatementDetails delegate;
	private final PreparedStatement statement;
	private final JdbcServices jdbcServices;

	public PrepareStatementDetailsAdaptor(PreparedStatementDetails delegate, PreparedStatement statement, JdbcServices jdbcServices) {
		this.delegate = delegate;
		this.statement = statement;
		this.jdbcServices = jdbcServices;
	}

	@Override
	public TableMapping getMutatingTableDetails() {
		return delegate.getMutatingTableDetails();
	}

	@Override
	public String getSqlString() {
		return delegate.getSqlString();
	}

	@Override
	public PreparedStatement getStatement() {
		return statement;
	}

	@Override
	public PreparedStatement resolveStatement() {
		try {
			delegate.getExpectation().prepare( statement );
			return statement;
		}
		catch (SQLException e) {
			throw jdbcServices.getSqlExceptionHelper()
					.convert( e, "Unable to prepare for expectation", getSqlString() );
		}
	}

	@Override
	public Expectation getExpectation() {
		return delegate.getExpectation();
	}

	@Override
	public boolean isCallable() {
		return delegate.isCallable();
	}

	@Override
	public void releaseStatement(SharedSessionContractImplementor session) {
		delegate.releaseStatement( session );
	}
}
