/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.annotations.tests;

import org.hibernate.reactive.annotations.EnableFor;
import org.hibernate.reactive.containers.DatabaseConfiguration;

import org.junit.jupiter.api.Test;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.junit.jupiter.api.Assertions.assertEquals;

@EnableFor(value = POSTGRESQL)
public class EnableForClassTest {

	@Test
	public void test() {
		// Throw exception if this test is database is NOT POSTGRESQL
		assertEquals( POSTGRESQL, DatabaseConfiguration.dbType() );
	}
}
