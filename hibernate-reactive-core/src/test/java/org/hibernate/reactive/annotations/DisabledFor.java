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
 * Test will be disabled for the selected {@link org.hibernate.reactive.containers.DatabaseConfiguration.DBType}
 *
 * @see EnabledFor
 */
@Inherited
@Retention( RetentionPolicy.RUNTIME )
@Target({ ElementType.TYPE, ElementType.METHOD})
@Repeatable( DisabledForDbTypes.class )
@ExtendWith( DisabledForDBTypeCondition.class )
public @interface DisabledFor {
	DatabaseConfiguration.DBType[] value();
	String reason() default "<undefined>";
}
