/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.annotations.tests;

import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.annotations.DisabledForDbTypes;
import org.hibernate.reactive.annotations.EnabledFor;
import org.hibernate.reactive.annotations.EnabledForDbTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

public class FilterByDbTypeOnMethodTest {

	@Test
	@DisabledFor(POSTGRESQL)
	public void testDisableOneDb() {
		// Throw exception if this test is run with POSTGRESQL database
		assertThat( dbType() ).isNotEqualTo( POSTGRESQL );
	}

	@Test
	@DisabledFor(value = {POSTGRESQL, MYSQL, DB2}, reason = "some reason")
	public void testDisableMultipleDbs() {
		// Throw exception if this test is run with POSTGRESQL database
		assertThat( dbType() ).isNotIn( POSTGRESQL, MYSQL, DB2 );
	}

	@Test
	@DisabledFor(value = MYSQL, reason = "some reason")
	@DisabledFor(value = POSTGRESQL, reason = "some reason")
	public void testDisabledRepeatable() {
		// Throw exception if this test is run with POSTGRESQL or MYSQL database
		assertThat( dbType() ).isNotIn( POSTGRESQL, MYSQL );
	}

	@Test
	@DisabledForDbTypes({
			@DisabledFor(MYSQL),
			@DisabledFor(POSTGRESQL)
	})
	public void testDisabledForGroup() {
		// Throw exception if this test is run with POSTGRESQL or MYSQL database
		assertThat( dbType() ).isNotIn( POSTGRESQL, MYSQL );
	}

	@Test
	@EnabledFor(POSTGRESQL)
	public void testEnabledForForOneDb() {
		assertThat( dbType() ).isEqualTo( POSTGRESQL );
	}

	@Test
	@EnabledFor(value = {POSTGRESQL, MYSQL, DB2}, reason = "some reason")
	public void testEnabledForMultipleDbs() {
		assertThat( dbType() ).isIn( POSTGRESQL, MYSQL, DB2 );
	}

	@Test
	@EnabledFor(value = MYSQL, reason = "some reason")
	@EnabledFor(value = POSTGRESQL, reason = "some reason")
	public void testEnabledForRepeatable() {
		assertThat( dbType() ).isIn( POSTGRESQL, MYSQL );
	}

	@Test
	@EnabledForDbTypes({
			@EnabledFor(MYSQL),
			@EnabledFor(POSTGRESQL)
	})
	public void testEnabledForForGroup() {
		assertThat( dbType() ).isIn( POSTGRESQL, MYSQL );
	}
}
