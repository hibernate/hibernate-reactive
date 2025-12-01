/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.generator.OnExecutionGenerator;
import org.hibernate.id.insert.InsertGeneratedIdentifierDelegate;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.id.insert.ReactiveGetGeneratedKeysDelegate;
import org.hibernate.reactive.id.insert.ReactiveInsertReturningDelegate;
import org.hibernate.reactive.id.insert.ReactiveUniqueKeySelectingDelegate;

import static org.hibernate.generator.EventType.INSERT;
import static org.hibernate.generator.values.internal.GeneratedValuesHelper.noCustomSql;
import static org.hibernate.reactive.generator.values.internal.ReactiveGeneratedValuesHelper.supportReactiveGetGeneratedKey;
import static org.hibernate.reactive.generator.values.internal.ReactiveGeneratedValuesHelper.supportsReturning;

public interface ReactiveOnExecutionGenerator extends OnExecutionGenerator {

	@Override
	default InsertGeneratedIdentifierDelegate getGeneratedIdentifierDelegate(EntityPersister persister) {
		final SessionFactoryImplementor factory = persister.getFactory();
		final Dialect dialect = factory.getJdbcServices().getDialect();
		// Hibernate ORM allows the selection of different strategies based on the property `hibernate.jdbc.use_get_generated_keys`.
		// But that's a specific JDBC property and with Vert.x we only have one viable option for each supported database.
		final boolean supportsInsertReturning = supportsReturning( dialect, INSERT );
		if ( supportsInsertReturning && noCustomSql( persister, INSERT ) ) {
			return new ReactiveInsertReturningDelegate( persister, INSERT );
		}
		else if ( supportReactiveGetGeneratedKey( dialect, persister.getGeneratedProperties( INSERT ) ) ) {
			return new ReactiveGetGeneratedKeysDelegate( persister, false, INSERT );
		}
		else {
			// let's just hope the entity has a @NaturalId!
			return new ReactiveUniqueKeySelectingDelegate( persister, getUniqueKeyPropertyNames( persister ), INSERT );
		}
	}

}
