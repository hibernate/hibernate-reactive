/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import java.sql.PreparedStatement;

import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.jdbc.Expectation;
import org.hibernate.sql.model.TableMapping;

public class PreparedStatementDetailsAdaptor implements PreparedStatementDetails {

	private final PreparedStatementDetails delegate;

	public PreparedStatementDetailsAdaptor(PreparedStatementDetails delegate) {
		this.delegate = delegate;
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
		return delegate.getStatement();
	}

	@Override
	public PreparedStatement resolveStatement() {
		return delegate.resolveStatement();
	}

	@Override
	public Expectation getExpectation() {
		return delegate.getExpectation();
	}

	@Override
	public void releaseStatement(SharedSessionContractImplementor session) {
		delegate.releaseStatement( session );
	}
}
