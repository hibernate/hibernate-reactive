/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id;

import org.hibernate.Incubating;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import java.util.concurrent.CompletionStage;

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
public interface ReactiveIdentifierGenerator<Id> {

	/**
	 * Returns a generated identifier, via a {@link CompletionStage}.
	 *
	 * @param session the reactive session
	 */
	CompletionStage<Id> generate(ReactiveConnectionSupplier session, Object entity);
}
