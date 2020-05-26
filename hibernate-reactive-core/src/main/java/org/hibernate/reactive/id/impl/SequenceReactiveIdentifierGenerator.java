/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.MappingException;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.id.Configurable;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.Type;

import java.util.Properties;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.id.impl.IdentifierGeneration.determineSequenceName;

/**
 * Support for JPA's {@link javax.persistence.SequenceGenerator}.
 */
public class SequenceReactiveIdentifierGenerator
		implements ReactiveIdentifierGenerator<Long>, Configurable {

	private String sql;
	private QualifiedName qualifiedSequenceName;

	@Override
	public void configure(Type type, Properties params, ServiceRegistry serviceRegistry) throws MappingException {
		JdbcEnvironment jdbcEnvironment = serviceRegistry.getService( JdbcEnvironment.class );
		Dialect dialect = jdbcEnvironment.getDialect();

		qualifiedSequenceName = determineSequenceName( params, serviceRegistry );

		// allow physical naming strategies a chance to kick in
		String renderedSequenceName = jdbcEnvironment.getQualifiedObjectNameFormatter()
				.format( qualifiedSequenceName, dialect );

		sql = dialect.getSequenceNextValString( renderedSequenceName );
	}

	@Override
	public CompletionStage<Long> generate(ReactiveSession session, Object entity) {
		return session.getReactiveConnection().selectLong( sql, new Object[0] );
	}
}
