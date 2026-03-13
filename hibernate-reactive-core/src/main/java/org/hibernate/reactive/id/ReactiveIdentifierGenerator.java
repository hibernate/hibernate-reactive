/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id;

import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.Incubating;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

/**
 * A replacement for {@link org.hibernate.id.IdentifierGenerator},
 * which supports a non-blocking method for obtaining the generated
 * identifier.
 * <p>
 * A custom generation strategy may be defined by implementing this
 * interface, and, optionally, {@link org.hibernate.id.Configurable},
 * and declaring the custom implementation class using
 * {@link org.hibernate.annotations.GenericGenerator}.
 *
 * @see IdentifierGenerator
 */
@Incubating
public interface ReactiveIdentifierGenerator<Id> extends IdentifierGenerator {

	/**
	 * Returns a generated identifier, via a {@link CompletionStage}.
	 *
	 * @param session the reactive session
	 */
	CompletionStage<Id> generate(ReactiveConnectionSupplier session, Object entity);

	default CompletionStage<Id> generate(ReactiveConnectionSupplier session, Object owner, Object currentValue, EventType eventType) {
		return generate( session, owner );
	}

	@Override
	default Id generate(
			SharedSessionContractImplementor session,
			Object owner,
			Object currentValue,
			EventType eventType){
		throw new HibernateException( "Detected call to non reactive method. Alternative reactive method: `generate(ReactiveConnectionSupplier, Object, Object, EventType)`" );
	}

	@Override
	default Object generate(SharedSessionContractImplementor session, Object object){
		throw new HibernateException( "Detected call to non reactive method. Alternative reactive method: `generate(ReactiveConnectionSupplier, Object)`" );
	}
}
