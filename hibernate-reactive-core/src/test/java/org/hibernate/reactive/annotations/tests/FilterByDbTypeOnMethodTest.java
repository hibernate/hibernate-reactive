/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.annotations.tests;

import org.hibernate.reactive.annotations.DisableFor;
import org.hibernate.reactive.annotations.DisableForGroup;
import org.hibernate.reactive.annotations.EnableFor;
import org.hibernate.reactive.annotations.EnableForGroup;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

public class FilterByDbTypeOnMethodTest {

	@Test
	@DisableFor(POSTGRESQL)
	public void testDisableOneDb() {
		// Throw exception if this test is run with POSTGRESQL database
		assertThat( dbType() ).isNotEqualTo( POSTGRESQL );
	}

	@Test
	@DisableFor(value = {POSTGRESQL, MYSQL, DB2}, reason = "some reason")
	public void testDisableMultipleDbs() {
		// Throw exception if this test is run with POSTGRESQL database
		assertThat( dbType() ).isNotIn( POSTGRESQL, MYSQL, DB2 );
	}

	@Test
	@DisableFor(value = MYSQL, reason = "some reason")
	@DisableFor(value = POSTGRESQL, reason = "some reason")
	public void testDisabledRepeatable() {
		// Throw exception if this test is run with POSTGRESQL or MYSQL database
		assertThat( dbType() ).isNotIn( POSTGRESQL, MYSQL );
	}

	@Test
	@DisableForGroup({
			@DisableFor(MYSQL),
			@DisableFor(POSTGRESQL)
	})
	public void testDisabledForGroup() {
		// Throw exception if this test is run with POSTGRESQL or MYSQL database
		assertThat( dbType() ).isNotIn( POSTGRESQL, MYSQL );
	}

	@Test
	@EnableFor(POSTGRESQL)
	public void testEnableForOneDb() {
		assertThat( dbType() ).isEqualTo( POSTGRESQL );
	}

	@Test
	@EnableFor(value = {POSTGRESQL, MYSQL, DB2}, reason = "some reason")
	public void testEnableMultipleDbs() {
		assertThat( dbType() ).isIn( POSTGRESQL, MYSQL, DB2 );
	}

	@Test
	@EnableFor(value = MYSQL, reason = "some reason")
	@EnableFor(value = POSTGRESQL, reason = "some reason")
	public void testEnableRepeatable() {
		assertThat( dbType() ).isIn( POSTGRESQL, MYSQL );
	}

	@Test
	@EnableForGroup({
			@EnableFor(MYSQL),
			@EnableFor(POSTGRESQL)
	})
	public void testEnableForGroup() {
		assertThat( dbType() ).isIn( POSTGRESQL, MYSQL );
	}
}
