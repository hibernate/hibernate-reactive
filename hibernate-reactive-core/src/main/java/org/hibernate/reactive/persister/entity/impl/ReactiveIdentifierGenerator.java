package org.hibernate.reactive.persister.entity.impl;

import org.hibernate.reactive.impl.ReactiveSessionInternal;

import java.util.concurrent.CompletionStage;

/**
 * A replacement for {@link org.hibernate.id.IdentifierGenerator},
 * which supports a non-blocking method for obtaining the generated
 * identifier.
 *
 * @see TableReactiveIdentifierGenerator
 * @see SequenceReactiveIdentifierGenerator
 */
@FunctionalInterface
public interface ReactiveIdentifierGenerator<Id> {
	/**
	 * Returns a generated identifier, via a {@link CompletionStage}.
	 */
	CompletionStage<Id> generate(ReactiveSessionInternal session);
}
