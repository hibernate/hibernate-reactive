/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.enhanced.DatabaseStructure;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

/**
 * Support for JPA's {@link jakarta.persistence.SequenceGenerator}.
 * <p>
 * This implementation supports block allocation, but does not
 * guarantee that generated identifiers are sequential.
 */
public class ReactiveSequenceIdentifierGenerator extends BlockingIdentifierGenerator implements IdentifierGenerator {

	public static final Object[] NO_PARAMS = new Object[0];
	private Dialect dialect;
	private QualifiedName qualifiedName;

	private String sql;
	private int increment;

	public ReactiveSequenceIdentifierGenerator() {
	}

	public ReactiveSequenceIdentifierGenerator(DatabaseStructure structure, RuntimeModelCreationContext creationContext) {
		qualifiedName = structure.getPhysicalName();
		increment = structure.getIncrementSize();
		dialect = creationContext.getDialect();
	}

	@Override
	protected int getBlockSize() {
		return increment;
	}

	@Override
	protected CompletionStage<Long> nextHiValue(ReactiveConnectionSupplier session) {
		return session.getReactiveConnection()
				.selectIdentifier( sql, NO_PARAMS, Long.class );
	}

	// Called after configure
	@Override
	public void initialize(SqlStringGenerationContext context) {
		String renderedSequenceName = context.format( qualifiedName );
		sql = dialect.getSequenceSupport().getSequenceNextValString( renderedSequenceName );
	}

	@Override
	public Object generate(SharedSessionContractImplementor session, Object object) throws HibernateException {
		// TODO: Do we need to implement this?
		throw new UnsupportedOperationException("Not yet implemented");
	}

	public QualifiedName getSequenceName() {
		return qualifiedName;
	}
}
