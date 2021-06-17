/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.util.impl;

/**
 * Similar to a {@link java.util.function.BiPredicate}
 * but one of the arguments is a primitive integer.
 *
 * @param <T> the type of the first argument of the function
 */
@FunctionalInterface
public interface IntBiPredicate<T> {
	boolean test(T value, int integer);
}
