/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.dialect.Dialect;
import org.hibernate.id.BulkInsertionCapableIdentifierGenerator;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.enhanced.DatabaseStructure;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

/**
 * Support for JPA's {@link jakarta.persistence.SequenceGenerator}.
 * <p>
 * Mimic a {@link org.hibernate.id.enhanced.SequenceStyleGenerator} with {@link org.hibernate.id.enhanced.SequenceStructure}.
 * <p>
 * This implementation supports block allocation, but does not
 * guarantee that generated identifiers are sequential.
 */
public class ReactiveSequenceIdentifierGenerator extends BlockingIdentifierGenerator
		implements IdentifierGenerator, BulkInsertionCapableIdentifierGenerator {

	public static final Object[] NO_PARAMS = new Object[0];

	private final Dialect dialect;
	private final QualifiedName qualifiedName;
	private final int increment;
	private final boolean supportsBulkInsertion;

	private String sql;

	public ReactiveSequenceIdentifierGenerator(DatabaseStructure structure, RuntimeModelCreationContext creationContext) {
		qualifiedName = structure.getPhysicalName();
		increment = structure.getIncrementSize();
		dialect = creationContext.getDialect();
		supportsBulkInsertion = structure.isPhysicalSequence();
	}

	@Override
	protected int getBlockSize() {
		return increment;
	}

	@Override
	protected CompletionStage<Long> nextHiValue(ReactiveConnectionSupplier session) {
		return session.getReactiveConnection().selectIdentifier( sql, NO_PARAMS, Long.class );
	}

	// Called after configure
	@Override
	public void initialize(SqlStringGenerationContext context) {
		sql = dialect.getSequenceSupport().getSequenceNextValString( context.format( qualifiedName ) );
	}

	@Override
	public boolean supportsBulkInsertionIdentifierGeneration() {
		return supportsBulkInsertion;
	}

	@Override
	public String determineBulkInsertionIdentifierGenerationSelectFragment(SqlStringGenerationContext context) {
		return context.getDialect().getSequenceSupport().getSelectSequenceNextValString( context.format( qualifiedName ) );
	}
}
