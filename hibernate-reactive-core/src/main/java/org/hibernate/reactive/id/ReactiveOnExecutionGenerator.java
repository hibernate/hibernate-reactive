/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id;

import org.hibernate.dialect.Dialect;
import org.hibernate.generator.OnExecutionGenerator;
import org.hibernate.id.insert.InsertGeneratedIdentifierDelegate;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.id.insert.ReactiveInsertReturningDelegate;

public interface ReactiveOnExecutionGenerator extends OnExecutionGenerator {

	@Override
	default InsertGeneratedIdentifierDelegate getGeneratedIdentifierDelegate(EntityPersister persister) {
		Dialect dialect = persister.getFactory().getJdbcServices().getDialect();
		// Hibernate ORM allows the selection of different strategies based on the property `hibernate.jdbc.use_get_generated_keys`.
		// But that's a specific JDBC property and with Vert.x we only have one viable option for each supported database.
		return new ReactiveInsertReturningDelegate( persister, dialect );
	}

}
