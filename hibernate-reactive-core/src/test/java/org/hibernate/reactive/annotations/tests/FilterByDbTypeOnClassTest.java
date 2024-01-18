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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

public class FilterByDbTypeOnClassTest {

	@Nested
	@DisableFor(value = POSTGRESQL, reason = "some reason")
	class DisableForOneDbType {
		@Test
		public void test() {
			assertThat( dbType() ).isNotIn( POSTGRESQL );
		}
	}

	@Nested
	@DisableFor({POSTGRESQL, MYSQL, DB2})
	class DisableForMultipleDbType {
		@Test
		public void test() {
			assertThat( dbType() ).isNotIn( POSTGRESQL, MYSQL, DB2 );
		}
	}

	@Nested
	@DisableFor(value = MYSQL, reason = "some reason")
	@DisableFor(value = POSTGRESQL, reason = "some reason")
	class DisabledForRepeatableTest {
		@Test
		public void testDisabledRepeatable() {
			assertThat( dbType() ).isNotIn( POSTGRESQL, MYSQL );
		}
	}

	@Nested
	@DisableForGroup({
			@DisableFor(MYSQL),
			@DisableFor(POSTGRESQL)
	})
	class DisabledForGroupTest {
		@Test
		public void testDisabledRepeatable() {
			assertThat( dbType() ).isNotIn( POSTGRESQL, MYSQL );
		}
	}

	@Nested
	@EnableFor(POSTGRESQL)
	class EnableForOneDbTypeTest {
		@Test
		public void test() {
			assertThat( dbType() ).isEqualTo( POSTGRESQL );
		}
	}

	@Nested
	@EnableFor(value = {POSTGRESQL, MYSQL, DB2}, reason = "some reason")
	class EnableForMultipleDbTypeTest {
		@Test
		public void test() {
			assertThat( dbType() ).isIn( POSTGRESQL, MYSQL, DB2 );
		}
	}

	@Nested
	@EnableFor(value = {POSTGRESQL, MYSQL, DB2}, reason = "some reason")
	class EnableRepeatableDbTypeTest {
		@Test
		public void test() {
			assertThat( dbType() ).isIn( POSTGRESQL, MYSQL, DB2 );
		}
	}

	@Nested
	@EnableForGroup({
			@EnableFor(MYSQL),
			@EnableFor(POSTGRESQL)
	})
	class EnableForGroupTest {
		@Test
		public void test() {
			assertThat( dbType() ).isIn( POSTGRESQL, MYSQL );
		}
	}
}
