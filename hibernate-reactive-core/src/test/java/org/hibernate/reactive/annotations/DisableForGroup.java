/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.hibernate.reactive.containers.DatabaseConfiguration;

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Annotation that allows disabling tests or test methods for specific database types.
 *
 * Types are defined in {@link DatabaseConfiguration.DBType} and can be
 * applied to a test class or a test method
 *
 * <pre>{@code
 *
 * @DisableForGroup( {
 *     @DisableFor(value = MYSQL, reason = "Reason #1"),
 *     @DisableFor(value = DB2, reason = "Reason #2")
 * } )
 * public class DisableDBsForClassTest {
 *
 * 		@Test
 * 		public void test(VertxTestContext context) {
 *          ....
 * 		}
 * }
 *
 * public class DisableDBsForMethodTest {
 *
 * 		@Test
 * 		@DisableForGroup( {
 *      	   @DisableFor(value = POSTGRES, reason = "Reason #3"),
 *      	   @DisableFor(value = MYSQL, reason = "Reason #4")
 *         } )
 * 		public void test(VertxTestContext context) {
 *          ....
 * 		}
 * }
 * }</pre>
 *
 */

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD})
@ExtendWith( DisableForDBTypeCondition.class )
public @interface DisableForGroup {
	DisableFor[] value();
}
