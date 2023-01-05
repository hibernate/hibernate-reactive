/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.spi.JdbcCoordinator;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.PostInsertIdentityPersister;
import org.hibernate.id.insert.GetGeneratedKeysDelegate;

import static org.hibernate.id.IdentifierGeneratorHelper.getGeneratedIdentity;

public class ReactiveGetGeneratedKeysDelegate extends GetGeneratedKeysDelegate {

	public ReactiveGetGeneratedKeysDelegate(PostInsertIdentityPersister persister, Dialect dialect) {
		super( persister, dialect );
	}

	@Override
	public Object performInsert(PreparedStatementDetails insertStatementDetails, JdbcValueBindings jdbcValueBindings, Object entity, SharedSessionContractImplementor session) {
		final JdbcServices jdbcServices = session.getJdbcServices();
		final JdbcCoordinator jdbcCoordinator = session.getJdbcCoordinator();
		final String insertSql = insertStatementDetails.getSqlString();

		jdbcServices.getSqlStatementLogger().logStatement( insertSql );

		final PreparedStatement insertStatement = insertStatementDetails.resolveStatement();
		jdbcValueBindings.beforeStatement( insertStatementDetails, session );

		try {
			jdbcCoordinator.getResultSetReturn().executeUpdate( insertStatement, insertSql );

			try {
				final ResultSet resultSet = insertStatement.getGeneratedKeys();
				try {
					return getGeneratedIdentity( getPersister().getNavigableRole().getFullPath(), resultSet, getPersister(), session );
				}
				catch (SQLException e) {
					throw jdbcServices.getSqlExceptionHelper()
							.convert( e, () -> String.format( Locale.ROOT, "Unable to extract generated key from generated-key for `%s`", getPersister().getNavigableRole().getFullPath() ), insertSql );
				}
				finally {
					if ( resultSet != null ) {
						jdbcCoordinator
								.getLogicalConnection()
								.getResourceRegistry()
								.release( resultSet, insertStatement );
					}
				}
			}
			finally {
				jdbcCoordinator.getLogicalConnection().getResourceRegistry().release( insertStatement );
			}
		}
		catch (SQLException e) {
			throw jdbcServices.getSqlExceptionHelper()
					.convert( e, "Unable to extract generated-keys ResultSet", insertSql );
		}
	}
}
