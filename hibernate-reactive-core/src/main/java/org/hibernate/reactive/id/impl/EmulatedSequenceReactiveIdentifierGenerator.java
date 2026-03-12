/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.id.BulkInsertionCapableIdentifierGenerator;
import org.hibernate.id.PersistentIdentifierGenerator;
import org.hibernate.id.enhanced.Optimizer;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.service.ServiceRegistry;

/**
 * Support for JPA's {@link jakarta.persistence.SequenceGenerator}
 * for databases which do not support sequences. Persistence is
 * managed via a table with just one row and one column.
 * <p>
 * Mimic a {@link SequenceStyleGenerator} with {@link org.hibernate.id.enhanced.TableStructure}.
 * <p>
 * This implementation supports block allocation, but does not
 * guarantee that generated identifiers are sequential.
 */
public class EmulatedSequenceReactiveIdentifierGenerator extends TableReactiveIdentifierGenerator
		implements BulkInsertionCapableIdentifierGenerator, PersistentIdentifierGenerator {

	private final SequenceStyleGenerator generator;

	public EmulatedSequenceReactiveIdentifierGenerator(SequenceStyleGenerator generator, RuntimeModelCreationContext runtimeModelCreationContext) {
		super( generator, runtimeModelCreationContext );
		this.generator = generator;
	}

	@Override
	public Optimizer getOptimizer() {
		// I don't think this is correct because the Optimizer is not reactive, but tests will fail for MySQL if
		// we don't return something (See tests running insert-select update queries with MySQL).
		return generator.getOptimizer();
	}

	@Override
	public boolean supportsBulkInsertionIdentifierGeneration() {
		return generator.supportsBulkInsertionIdentifierGeneration();
	}

	@Override
	public String determineBulkInsertionIdentifierGenerationSelectFragment(SqlStringGenerationContext context) {
		return generator.determineBulkInsertionIdentifierGenerationSelectFragment( context );
	}

	@Override
	protected Boolean determineStoreLastUsedValue(ServiceRegistry serviceRegistry) {
		return false;
	}
}
