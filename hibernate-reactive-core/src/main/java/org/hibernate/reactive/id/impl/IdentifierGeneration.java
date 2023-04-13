/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import java.lang.invoke.MethodHandles;

import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.type.descriptor.java.IntegerJavaType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.java.LongJavaType;
import org.hibernate.type.descriptor.java.ShortJavaType;
import org.hibernate.type.descriptor.java.StringJavaType;


public class IdentifierGeneration {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public static Object castToIdentifierType(Object generatedId, EntityPersister persister) {
		return generatedId instanceof Long
				? castLongIdToIdentifierType( (Long) generatedId, persister )
				: generatedId;
	}

	private static Object castLongIdToIdentifierType(Long longId, EntityPersister persister) {
		if ( longId <= 0 ) {
			throw LOG.generatedIdentifierSmallerOrEqualThanZero( longId );
		}

		final JavaType<?> identifierType = persister.getIdentifierMapping().getJavaType();
		if ( identifierType == LongJavaType.INSTANCE ) {
			return longId;
		}
		if ( identifierType == IntegerJavaType.INSTANCE ) {
			validateMaxValue( persister, longId, Integer.MAX_VALUE );
			return longId.intValue();

		}
		if ( identifierType == ShortJavaType.INSTANCE ) {
			validateMaxValue( persister, longId, Short.MAX_VALUE );
			return longId.shortValue();
		}
		if ( identifierType == StringJavaType.INSTANCE ) {
			return longId.toString();
		}

		throw LOG.cannotGenerateIdentifiersOfType(
				identifierType.getJavaType().getTypeName(),
				persister.getEntityName()
		);
	}

	private static void validateMaxValue(EntityPersister persister, Long id, int maxValue) {
		if ( id > maxValue ) {
			throw LOG.generatedIdentifierTooBigForTheField(
					persister.getEntityName(),
					persister.getIdentifierType().getReturnedClass().getSimpleName(),
					id
			);
		}
	}
}
