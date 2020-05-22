package org.hibernate.reactive.id;

import org.hibernate.Incubating;
import org.hibernate.reactive.id.impl.SequenceReactiveIdentifierGenerator;
import org.hibernate.reactive.id.impl.TableReactiveIdentifierGenerator;
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
@Incubating
@FunctionalInterface
public interface ReactiveIdentifierGenerator<Id> {
	/**
	 * Returns a generated identifier, via a {@link CompletionStage}.
	 *
	 * @param session the reactive session
	 */
	CompletionStage<Id> generate(ReactiveSessionInternal session, Object entity);
}
