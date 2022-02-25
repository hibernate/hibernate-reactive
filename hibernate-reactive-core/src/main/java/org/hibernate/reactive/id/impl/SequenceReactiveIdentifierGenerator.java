/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.id.Configurable;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.Type;

import java.util.Properties;
import java.util.concurrent.CompletionStage;

import static org.hibernate.internal.util.config.ConfigurationHelper.getInt;
import static org.hibernate.reactive.id.impl.IdentifierGeneration.determineSequenceName;

/**
 * Support for JPA's {@link jakarta.persistence.SequenceGenerator}.
 * <p>
 * This implementation supports block allocation, but does not
 * guarantee that generated identifiers are sequential.
 */
public class SequenceReactiveIdentifierGenerator
		extends BlockingIdentifierGenerator implements Configurable {

	public static final Object[] NO_PARAMS = new Object[0];

	private String sql;

	private int increment;

	@Override
	protected int getBlockSize() {
		return increment;
	}

	@Override
	protected CompletionStage<Long> nextHiValue(ReactiveConnectionSupplier session) {
		return session.getReactiveConnection().selectIdentifier( sql, NO_PARAMS, Long.class ).thenApply( this::next );
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

	protected int determineIncrementForSequenceEmulation(Properties params) {
		return getInt( SequenceStyleGenerator.INCREMENT_PARAM, params, SequenceStyleGenerator.DEFAULT_INCREMENT_SIZE );
	}
}
