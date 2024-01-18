/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.annotations.tests;

import org.hibernate.reactive.annotations.EnableFor;
import org.hibernate.reactive.annotations.EnableForGroup;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

@EnableForGroup({
		@EnableFor(value = MYSQL),
		@EnableFor(value = POSTGRESQL)
})
public class EnableForGroupClassTest {

	@Test
	public void test() {
		// Throw exception if this test is run with POSTGRESQL or MYSQL database
		assertThat( dbType() ).isIn( MYSQL, POSTGRESQL );
	}
}
