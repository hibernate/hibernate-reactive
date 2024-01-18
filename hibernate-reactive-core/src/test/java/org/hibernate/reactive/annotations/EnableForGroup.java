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
 * Annotation that allows enabling tests or test methods only for specific database types.
 *
 * Types are defined in {@link DatabaseConfiguration.DBType} and can be
 * applied to a test class or a test method
 *
 * <pre>{@code
 *
 * @EnableForGroup( {
 *     @EnableFor( MYSQL ),
 *     @EnableFor( DB2 )
 * } )
 * public class EnableDBsForClassTest {
 *
 * 		@Test
 * 		public void test(VertxTestContext context) {
 *          ....
 * 		}
 * }
 *
 * public class EnableDBsForMethodTest {
 *
 * 		@Test
 * 		@EnableForGroup( {
 *      	   @EnableFor( POSTGRES),
 *      	   @EnableFor( MYSQL )
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
@ExtendWith( EnableForDBTypeCondition.class )
public @interface EnableForGroup {
	EnableFor[] value();
}
