/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.generated.spi.GeneratedValuesSupport;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.generator.OnExecutionGenerator;
import org.hibernate.id.insert.InsertGeneratedIdentifierDelegate;
import org.hibernate.id.insert.UniqueKeySelectingDelegate;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.id.insert.ReactiveGetGeneratedKeysDelegate;
import org.hibernate.reactive.id.insert.ReactiveInsertReturningDelegate;

import static org.hibernate.generator.EventType.INSERT;
import static org.hibernate.generator.values.internal.GeneratedValuesHelper.noCustomSql;

public interface ReactiveOnExecutionGenerator extends OnExecutionGenerator {

	@Override
	default InsertGeneratedIdentifierDelegate getGeneratedIdentifierDelegate(EntityPersister persister) {
		final SessionFactoryImplementor factory = persister.getFactory();
		final Dialect dialect = factory.getJdbcServices().getDialect();
		final var generatedValuesSupport = dialect.getGeneratedValuesSupport();
		if ( generatedValuesSupport.supports( GeneratedValuesSupport.Capability.ARBITRARY_GENERATED_KEYS )
				&& factory.getSessionFactoryOptions().isGetGeneratedKeysEnabled() ) {
			return new ReactiveGetGeneratedKeysDelegate( persister, false, INSERT );
		}
		if ( generatedValuesSupport.supports( GeneratedValuesSupport.Capability.INSERT_RETURNING )
				&& noCustomSql( persister, INSERT ) ) {
			return new ReactiveInsertReturningDelegate( persister, INSERT );
		}
		// let's just hope the entity has a @NaturalId!
		return new UniqueKeySelectingDelegate( persister, getUniqueKeyPropertyNames( persister ), INSERT );
	}

}
