/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.testing;


import org.hibernate.reactive.containers.DatabaseConfiguration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.hibernate.reactive.testing.DBSelectionExtension.runOnlyFor;
import static org.hibernate.reactive.testing.DBSelectionExtension.skipTestsFor;

public class DatabaseSelectionRuleTests {

	public static class SkipDBTest {
		@RegisterExtension
		public DBSelectionExtension dbSelection = skipTestsFor( POSTGRESQL );

		@Test
		public void shouldSkipPostgres() {
			assertThat( dbType() ).isNotEqualTo( POSTGRESQL );
		}
	}

	public static class SkipMultipleDBsTest {
		@RegisterExtension
		public DBSelectionExtension skipDbs = skipTestsFor( POSTGRESQL, MYSQL, MARIA );

		@Test
		public void shouldSkipPostgresMySQLAndMaria() {
			assertThat( dbType() ).isNotIn( POSTGRESQL, MYSQL, MARIA );
		}
	}

	public static class RunOnlyOnDBTest {
		@RegisterExtension
		public DBSelectionExtension skipDbs = runOnlyFor( POSTGRESQL );

		@Test
		public void shouldOnlyRunForPostgres() {
			assertThat( dbType() ).isEqualTo( POSTGRESQL );
		}
	}

	public static class RunOnlyOnDMultipleDBsTest {
		@RegisterExtension
		public DBSelectionExtension rule = runOnlyFor( POSTGRESQL, MYSQL, MARIA );

		@Test
		public void shouldOnlyRunForPostgresOrMySql() {
			assertThat( DatabaseConfiguration.dbType() ).isIn( POSTGRESQL, MYSQL, MARIA );
		}
	}
}
