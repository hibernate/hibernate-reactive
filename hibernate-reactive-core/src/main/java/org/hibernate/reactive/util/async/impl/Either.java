/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.util.async.impl;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Copy of com.ibm.asyncutil.util.Either from com.ibm.async:asyncutil:0.1.0
 * without all the methods and imports we don't need for Hibernate Reactive.
 *
 * A container type that contains either an L or an R type object. An Either indicating a left value
 * can be constructed with {@link #left() Either.left(value)}, a right with {@link #right()
 * Either.right(value)}.
 *
 * <p>
 * By convention, if using this class to represent a result that may be an error or a success, the
 * error type should be in the left position and the success type should be in the right position
 * (mnemonic: "right" also means correct). As a result, this class implements monadic methods
 * (similar to those on {@link Optional}, {@link java.util.stream.Stream}, etc) for working on the R
 * type.
 *
 * <pre>
 * {@code
 * Either<Exception, String> tryGetString();
 * Either<Exception, Integer> tryParse(String s);
 * Double convert(Integer i);
 *
 * // if any intermediate step fails, will just be an Either.left(exception).
 * Either<Exception, Double> eitherDouble = tryGetString() // Either<Exception, String>
 *  .flatMap(tryParse) // Either<Exception, Integer>
 *  .map(convert); // Either<Exception, Double>
 *
 * }
 * </pre>
 *
 * @author Ravi Khadiwala
 * @param <L> The left type, by convention the error type if being used to represent possible errors
 * @param <R> The right type, by convention the success type if being used to represent possible
 *        errors
 */
public interface Either<L, R> {

  /**
   * Whether this container holds an L type
   *
   * @return true if this is a Left, false otherwise
   */
  default boolean isLeft() {
    return fold(left -> true, right -> false);
  }

  /**
   * Whether this container holds an R type
   *
   * @return true if this is a Right, false otherwise
   */
  default boolean isRight() {
    return fold(left -> false, right -> true);
  }

  /**
   * Applies exactly one of the two provided functions to produce a value of type {@code V}. For
   * example, applying an int function or defaulting to zero on error:
   *
   * <pre>
   * {@code
   * {
   *   Either<Exception, Integer> either = tryGetInteger();
   *   int halvedOrZero = either.fold(e -> 0, i -> i / 2);
   * }}
   * </pre>
   *
   * @param leftFn a function the produces a V from an L, only applied if {@code this} contains an L
   *        type
   * @param rightFn a function the produces a V from an R, only applied if {@code this} contains an
   *        R type
   * @param <V> the return type
   * @return a {@code V} value produced by {@code leftFn} if {@code this} contained an L, produced
   *         by {@code rightFn} otherwise.
   * @throws NullPointerException if the function to be applied is null
   */
  <V> V fold(
      final Function<? super L, ? extends V> leftFn,
      final Function<? super R, ? extends V> rightFn);

  /**
   * Calls exactly one of the two provided functions with an L or an R
   *
   * @param leftConsumer a function that consumes an L, only applied if {@code this} contains an L
   *        type
   * @param rightConsumer a function the consumes an R, only applied if {@code this} contains an R
   *        type
   * @throws NullPointerException if the function to be applied is null
   */
  default void forEach(
      final Consumer<? super L> leftConsumer, final Consumer<? super R> rightConsumer) {
    fold(
        left -> {
          leftConsumer.accept(left);
          return null;
        },
        right -> {
          rightConsumer.accept(right);
          return null;
        });
  }

  /**
   * Creates a new Either possibly of two new and distinct types, by applying the provided
   * transformation functions.
   *
   * <pre>
   * {@code
   * Either<Double, Integer> result;
   * Either<String, Boolean> x = result.map(d -> d.toString(), i -> i % 2 == 0)
   * }
   * </pre>
   *
   * @param leftFn a function that takes an L and produces an A, to be applied if {@code this}
   *        contains an L
   * @param rightFn a function that takes an R and produces and B, to be applied if {@code this}
   *        contains an R
   * @param <A> the left type of the returned Either
   * @param <B> the right type of the returned Either
   * @return a new Either, containing an A resulting from the application of leftFn if {@code this}
   *         contained an L, or containing a B resulting from the application of rightFn otherwise
   * @throws NullPointerException if the function to be applied is null
   */
  default <A, B> Either<A, B> map(
      final Function<? super L, ? extends A> leftFn,
      final Function<? super R, ? extends B> rightFn) {
    return fold(
        left -> Either.left(leftFn.apply(left)), right -> Either.right(rightFn.apply(right)));
  }

  /**
   * Transforms the right type of {@code this}, producing an Either of the transformed value if
   * {@code this} contained right, or an Either of the original left value otherwise. For example,
   * if we have either a Double or an error, convert it into an Integer if it's a Double, then
   * convert it to a String if it's an Integer:
   *
   * <pre>
   * {@code
   * Either<Exception, Double> eitherDouble;
   * Either<Exception, String> eitherString = eitherDouble.map(Double::intValue).map(Integer::toString)
   * }
   * </pre>
   *
   * @param fn function that takes an R value and produces a V value, to be applied if {@code this}
   *        contains an R
   * @param <V> the right type of the returned Either
   * @return an Either containing the transformed value produced by {@code fn} if {@code this}
   *         contained an R, or the original L value otherwise.
   */
  default <V> Either<L, V> map(final Function<? super R, ? extends V> fn) {
    return fold(Either::left, r -> Either.right(fn.apply(r)));
  }

  /**
   * Transforms the right type of {@code this}, producing a right Either of type {@code V} if {@code
   * this} was right <i>and</i> {@code f} produced a right Either, or a left Either otherwise. For
   * example, if we have either a String or an error, attempt to parse into an Integer (which could
   * potentially itself produce an error):
   *
   * <pre>
   * {@code
   * Either<Exception, Integer> tryParse(String s);
   * Either<Exception, String> eitherString;
   *
   * // will be an exception if eitherString was an exception
   * // or if tryParse failed, otherwise will be an Integer
   * Either<Exception, Integer> e = eitherString.flatMap(tryParse);
   *
   * }
   * </pre>
   *
   * @param f a function that takes an R value and produces an Either of a L value or a V value
   * @param <V> the right type of the returned Either
   * @return an Either containing the right value produced by {@code f} if both {@code this} and the
   *         produced Either were right, the left value of the produced Either if {@code this} was
   *         right but the produced value wasn't, or the original left value if {@code this} was
   *         left.
   */
  default <V> Either<L, V> flatMap(final Function<? super R, ? extends Either<L, V>> f) {
    return fold(Either::left, f);
  }

  /**
   * Optionally gets the left value of {@code this} if it exists
   *
   * @return a present {@link Optional} of an L if {@code this} contains an L, an empty one
   *         otherwise.
   * @throws NullPointerException if the left value of {@code this} is present and null
   */
  default Optional<L> left() {
    return fold(Optional::of, r -> Optional.empty());
  }

  /**
   * Optionally gets the right value of {@code this} if it exists
   *
   * @return a present {@link Optional} of an R if {@code this} contains an R, an empty one
   *         otherwise.
   * @throws NullPointerException if the right value of {@code this} is present and null
   */
  default Optional<R> right() {
    return fold(l -> Optional.empty(), Optional::of);
  }

  /**
   * Constructs an Either with a left value
   *
   * @param a the left element of the new Either
   * @return an Either with a left value
   */
  static <A, B> Either<A, B> left(final A a) {
    return new Either<A, B>() {

      @Override
      public <V> V fold(
          final Function<? super A, ? extends V> leftFn,
          final Function<? super B, ? extends V> rightFn) {
        return leftFn.apply(a);
      }

      @Override
      public String toString() {
        if (a == null) {
          return "Left (null type): null";
        } else {
          return String.format("Left (%s): %s", a.getClass(), a);
        }
      }
    };
  }

  /**
   * Constructs an Either with a right value
   *
   * @param b the right element of the new Either
   * @return An Either with a right value
   */
  static <A, B> Either<A, B> right(final B b) {
    return new Either<A, B>() {

      @Override
      public <V> V fold(
          final Function<? super A, ? extends V> leftFn,
          final Function<? super B, ? extends V> rightFn) {
        return rightFn.apply(b);
      }

      @Override
      public String toString() {
        if (b == null) {
          return "Right (null type): null";
        } else {
          return String.format("Right (%s): %s", b.getClass(), b);
        }
      }
    };
  }
}
