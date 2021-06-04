/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.util.impl;

/**
 * Similar to a {@link java.util.function.BiFunction}
 * but one of the arguments is a primitive integer.
 *
 * @param <T> the type of the first argument of the function
 * @param <R> the result the function
 */
@FunctionalInterface
public interface IntBiFunction<T, R> {
	R apply(T value, int integer);
}
