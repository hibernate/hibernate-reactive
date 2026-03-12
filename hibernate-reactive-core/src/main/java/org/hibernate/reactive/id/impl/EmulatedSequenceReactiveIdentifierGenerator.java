/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.id.enhanced.TableStructure;
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
public class EmulatedSequenceReactiveIdentifierGenerator extends TableReactiveIdentifierGenerator {

	public EmulatedSequenceReactiveIdentifierGenerator(TableStructure structure, RuntimeModelCreationContext runtimeModelCreationContext) {
		super( structure, runtimeModelCreationContext );
	}

	@Override
	protected Boolean determineStoreLastUsedValue(ServiceRegistry serviceRegistry) {
		return false;
	}
}
