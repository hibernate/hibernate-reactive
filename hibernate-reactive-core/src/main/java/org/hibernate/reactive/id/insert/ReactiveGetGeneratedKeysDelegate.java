/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;


import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.PostInsertIdentityPersister;
import org.hibernate.id.insert.GetGeneratedKeysDelegate;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;


public class ReactiveGetGeneratedKeysDelegate extends GetGeneratedKeysDelegate {

	public ReactiveGetGeneratedKeysDelegate(PostInsertIdentityPersister persister, Dialect dialect, boolean inferredKeys) {
		super( persister, dialect, inferredKeys );
	}

	@Override
	public Object performInsert(PreparedStatementDetails insertStatementDetails, JdbcValueBindings jdbcValueBindings, Object entity, SharedSessionContractImplementor session) {
		// FIXME: I should be able to generate the sql string beforehand
		final Class<?> idType = getPersister().getIdentifierType().getReturnedClass();
		final String identifierColumnName = getPersister().getIdentifierColumnNames()[0];
		final String insertSql = adaptQuery( insertStatementDetails, identifierColumnName );

		final JdbcServices jdbcServices = session.getJdbcServices();
		jdbcServices.getSqlStatementLogger().logStatement( insertSql );

		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( insertStatementDetails, statement, session.getJdbcServices() );
			jdbcValueBindings.beforeStatement( details, session );
		} );

		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		return reactiveConnection.insertAndSelectIdentifier( insertSql, params, idType, identifierColumnName );
	}

	private static String adaptQuery(PreparedStatementDetails insertStatementDetails, String identityColumnName) {
		final String insertSql = insertStatementDetails.getSqlString();
		return insertSql + " returning " + identityColumnName ;
	}
}
