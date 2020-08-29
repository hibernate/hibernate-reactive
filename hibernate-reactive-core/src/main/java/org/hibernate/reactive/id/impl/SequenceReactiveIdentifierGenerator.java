/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.id.Configurable;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.Type;

import java.util.Properties;
import java.util.concurrent.CompletionStage;

import static org.hibernate.internal.util.config.ConfigurationHelper.getInt;
import static org.hibernate.reactive.id.impl.IdentifierGeneration.determineSequenceName;

/**
 * Support for JPA's {@link javax.persistence.SequenceGenerator}.
 * <p>
 * This implementation supports block allocation, but does not
 * guarantee that generated identifiers are sequential.
 */
public class SequenceReactiveIdentifierGenerator
		implements ReactiveIdentifierGenerator<Long>, Configurable {

	public static final Object[] NO_PARAMS = new Object[0];

	private String sql;

	private int increment;

	private int loValue;
	private long hiValue;

	private synchronized long next() {
		return loValue>0 && loValue<increment
				? hiValue + loValue++
				: -1; //flag value indicating that we need to hit db
	}

	private synchronized long next(long hi) {
		hiValue = hi;
		loValue = 1;
		return hi;
	}

	@Override
	public void configure(Type type, Properties params, ServiceRegistry serviceRegistry) {
		JdbcEnvironment jdbcEnvironment = serviceRegistry.getService( JdbcEnvironment.class );
		Dialect dialect = jdbcEnvironment.getDialect();

		QualifiedName qualifiedSequenceName = determineSequenceName( params, serviceRegistry );

		// allow physical naming strategies a chance to kick in
		String renderedSequenceName = jdbcEnvironment.getQualifiedObjectNameFormatter()
				.format( qualifiedSequenceName, dialect );

		increment = determineIncrementForSequenceEmulation( params );

		sql = dialect.getSequenceNextValString( renderedSequenceName );
	}

	@Override
	public CompletionStage<Long> generate(ReactiveConnectionSupplier session, Object entity) {
		long local = next();
		if ( local >= 0 ) {
			return CompletionStages.completedFuture(local);
		}

		return session.getReactiveConnection()
				.selectLong( sql, NO_PARAMS )
				.thenApply( this::next );
	}

	protected int determineIncrementForSequenceEmulation(Properties params) {
		return getInt( SequenceStyleGenerator.INCREMENT_PARAM, params, SequenceStyleGenerator.DEFAULT_INCREMENT_SIZE );
	}
}
