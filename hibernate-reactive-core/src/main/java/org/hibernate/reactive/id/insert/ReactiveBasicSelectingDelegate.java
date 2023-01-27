/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletionStage;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.PostInsertIdentityPersister;
import org.hibernate.id.insert.BasicSelectingDelegate;
import org.hibernate.id.insert.Binder;

public class ReactiveBasicSelectingDelegate extends BasicSelectingDelegate implements ReactiveAbstractSelectingDelegate {

	public ReactiveBasicSelectingDelegate(PostInsertIdentityPersister persister, Dialect dialect) {
		super( persister, dialect );
	}

	@Override
	public CompletionStage<Object> reactivePerformInsert(
			String insertSQL,
			SharedSessionContractImplementor session,
			Binder binder) {
		throw LOG.notYetImplemented();
	}

	@Override
	public String getSelectSQL() {
		return super.getSelectSQL();
	}

	@Override
	public void bindParameters(Object entity, PreparedStatement ps, SharedSessionContractImplementor session) {
		try {
			super.bindParameters( entity, ps, session );
		}
		catch (SQLException e) {
			throw new RuntimeException( e );
		}
	}

	@Override
	public Object extractGeneratedValue(ResultSet resultSet, SharedSessionContractImplementor session) {
		try {
			return super.extractGeneratedValue( resultSet, session );
		}
		catch (SQLException e) {
			throw new RuntimeException( e );
		}
	}
}
