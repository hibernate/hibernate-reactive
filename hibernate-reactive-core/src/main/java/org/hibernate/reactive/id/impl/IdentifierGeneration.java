/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.Generator;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.type.descriptor.java.IntegerJavaType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.java.LongJavaType;
import org.hibernate.type.descriptor.java.ShortJavaType;
import org.hibernate.type.descriptor.java.StringJavaType;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

public class IdentifierGeneration {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@SuppressWarnings("unchecked")
	public static CompletionStage<Object> generateId(Object entity, EntityPersister persister, ReactiveConnectionSupplier connectionSupplier, SharedSessionContractImplementor session) {
		Generator generator = persister.getGenerator();
		return generator instanceof ReactiveIdentifierGenerator
				? ( (ReactiveIdentifierGenerator<Object>) generator ).generate( connectionSupplier, entity )
				: completedFuture( generator.generatesSometimes() );
	}

	public static Object assignIdIfNecessary(Object generatedId, Object entity, EntityPersister persister, SharedSessionContractImplementor session) {
		if ( generatedId != null ) {
			return castToIdentifierType( generatedId, persister );
		}

		Object assignedId = persister.getIdentifier( entity, session );
		if ( assignedId == null ) {
			throw LOG.idMustBeAssignedBeforeSave( persister.getEntityName() );
		}
		return assignedId;
	}

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

		throw LOG.cannotGenerateIdentifiersOfType( identifierType.getJavaType().getTypeName(), persister.getEntityName() );
	}

	private static void validateMaxValue(EntityPersister persister, Long id, int maxValue) {
		if ( id > maxValue ) {
			throw LOG.generatedIdentifierTooBigForTheField(persister.getEntityName(), persister.getIdentifierType().getReturnedClass().getSimpleName(), id );
		}
	}
}
