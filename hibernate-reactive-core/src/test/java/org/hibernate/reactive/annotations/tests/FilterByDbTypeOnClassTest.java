/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.annotations.tests;

import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.annotations.DisabledForDbTypes;
import org.hibernate.reactive.annotations.EnabledFor;
import org.hibernate.reactive.annotations.EnabledForDbTypes;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

public class FilterByDbTypeOnClassTest {

	@Nested
	@DisabledFor(value = POSTGRESQL, reason = "some reason")
	class DisabledForOneDbType {
		@Test
		public void test() {
			assertThat( dbType() ).isNotIn( POSTGRESQL );
		}
	}

	@Nested
	@DisabledFor({POSTGRESQL, MYSQL, DB2})
	class DisabledForMultipleDbType {
		@Test
		public void test() {
			assertThat( dbType() ).isNotIn( POSTGRESQL, MYSQL, DB2 );
		}
	}

	@Nested
	@DisabledFor(value = MYSQL, reason = "some reason")
	@DisabledFor(value = POSTGRESQL, reason = "some reason")
	class DisabledForRepeatableTest {
		@Test
		public void testDisabledRepeatable() {
			assertThat( dbType() ).isNotIn( POSTGRESQL, MYSQL );
		}
	}

	@Nested
	@DisabledForDbTypes({
			@DisabledFor(MYSQL),
			@DisabledFor(POSTGRESQL)
	})
	class DisabledForGroupTest {
		@Test
		public void testDisabledRepeatable() {
			assertThat( dbType() ).isNotIn( POSTGRESQL, MYSQL );
		}
	}

	@Nested
	@EnabledFor(POSTGRESQL)
	class EnabledForForOneDbTypeTest {
		@Test
		public void test() {
			assertThat( dbType() ).isEqualTo( POSTGRESQL );
		}
	}

	@Nested
	@EnabledFor(value = {POSTGRESQL, MYSQL, DB2}, reason = "some reason")
	class EnabledForForMultipleDbTypeTest {
		@Test
		public void test() {
			assertThat( dbType() ).isIn( POSTGRESQL, MYSQL, DB2 );
		}
	}

	@Nested
	@EnabledFor(value = {POSTGRESQL, MYSQL, DB2}, reason = "some reason")
	class EnabledForRepeatableDbTypeTest {
		@Test
		public void test() {
			assertThat( dbType() ).isIn( POSTGRESQL, MYSQL, DB2 );
		}
	}

	@Nested
	@EnabledForDbTypes({
			@EnabledFor(MYSQL),
			@EnabledFor(POSTGRESQL)
	})
	class EnabledForForGroupTest {
		@Test
		public void test() {
			assertThat( dbType() ).isIn( POSTGRESQL, MYSQL );
		}
	}
}
