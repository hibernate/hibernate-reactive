/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.spi;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.query.spi.NativeQueryInterpreter;
import org.hibernate.metamodel.model.domain.spi.JpaMetamodelImplementor;
import org.hibernate.query.criteria.ValueHandlingMode;
import org.hibernate.query.named.NamedObjectRepository;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.service.ServiceRegistry;

public class ReactiveQueryEngine extends QueryEngine {

	public ReactiveQueryEngine(
			String uuid,
			String name,
			JpaMetamodelImplementor jpaMetamodel,
			ValueHandlingMode criteriaValueHandlingMode,
			int preferredSqlTypeCodeForBoolean,
			boolean useStrictJpaCompliance,
			NamedObjectRepository namedObjectRepository,
			NativeQueryInterpreter nativeQueryInterpreter,
			Dialect dialect,
			ServiceRegistry serviceRegistry) {
		super(
				uuid,
				name,
				jpaMetamodel,
				criteriaValueHandlingMode,
				preferredSqlTypeCodeForBoolean,
				useStrictJpaCompliance,
				namedObjectRepository,
				nativeQueryInterpreter,
				dialect,
				serviceRegistry
		);
	}

	@Override
	public NamedObjectRepository getNamedObjectRepository() {
		return super.getNamedObjectRepository();
	}
}
