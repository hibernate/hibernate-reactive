/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.hibernate.reactive.containers.DatabaseConfiguration;

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Annotation that allows disabling tests or test methods only for specific database types.
 * <p>
 * Types are defined in {@link DatabaseConfiguration.DBType} and can be
 * applied to a test class or a test method
 *
 * <pre>{@code
 *
 * @EnableFor( MYSQL )
 * public class EnableDBForClassTest {
 *
 * 		@Test
 * 		public void test(VertxTestContext context) {
 *          ....
 * 		}
 * }
 *
 * public class EnableDBForMethodTest {
 *
 * 		@Test
 * 		@EnableFor( value = POSTGRES, "optional reason" )
 * 		public void test(VertxTestContext context) {
 *          ....
 * 		}
 * }
 * }</pre>
 *
 */

@SuppressWarnings("JavadocReference")
@Inherited
@Retention( RetentionPolicy.RUNTIME )
@Target({ ElementType.TYPE, ElementType.METHOD})
@Repeatable( EnableForGroup.class )
@ExtendWith( EnableForDBTypeCondition.class )
public @interface EnableFor {
	DatabaseConfiguration.DBType value();
	String reason() default "<undefined>";
}
