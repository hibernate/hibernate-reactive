/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.annotations.DisabledFor;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor(value = MYSQL, reason = "See https://github.com/hibernate/hibernate-reactive/issues/1525")
@DisabledFor(value = COCKROACHDB, reason = "we don't have permission to create schema in the CI!")
public class SequenceGeneratorTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( SequenceId.class );
	}

	@Test
	public void testSequenceGenerator(VertxTestContext context) {
		SequenceId b = new SequenceId();
		b.string = "Hello World";

		test( context, openSession()
				.thenCompose( s -> s.persist( b ).thenCompose( v -> s.flush() ) )
				.thenCompose( v -> openSession() )
				.thenCompose( s2 -> s2
						.find( SequenceId.class, b.getId() )
						.thenAccept( bb -> {
							assertThat( bb ).isNotNull();
							assertThat( bb.id ).isEqualTo( 5 );
							assertThat( bb.string ).isEqualTo( b.string );
							assertThat( bb.version ).isEqualTo( 0 );
							bb.string = "Goodbye";
						} )
						.thenCompose( vv -> s2.flush() )
						.thenCompose( vv -> s2.find( SequenceId.class, b.getId() ) )
						.thenAccept( bt -> assertThat( bt.version ).isEqualTo( 1 ) ) )
				.thenCompose( v -> openSession() )
				.thenCompose( s3 -> s3.find( SequenceId.class, b.getId() ) )
				.thenAccept( bb -> {
					assertThat( bb.version ).isEqualTo( 1 );
					assertThat( bb.string ).isEqualTo( "Goodbye" );
				} )
		);
	}

	public static class SequenceGeneratorDefaultSchemaTest extends SequenceGeneratorTest {

		@Override
		protected Configuration constructConfiguration() {
			Configuration configuration = super.constructConfiguration();
			configuration.setProperty( Settings.DEFAULT_SCHEMA, schemaName() );
			configuration.setProperty( Settings.HBM2DDL_CREATE_SCHEMAS, "true" );
			return configuration;
		}

		/**
		 * Oracle Database automatically creates a schema when you create a user,
		 * for the other databases we test with a schema that doesn't match the username.
		 */
		private String schemaName() {
			return dbType() == ORACLE
					? DatabaseConfiguration.USERNAME
					: "hr";
		}
	}

	@Entity(name = "SequenceId")
	@SequenceGenerator(name = "seq", sequenceName = "test_id_seq", initialValue = 5, allocationSize = 1)
	@Table(name = "SequenceId")
	public static class SequenceId {
		@Id
		@GeneratedValue(generator = "seq")
		Integer id;
		@Version
		Integer version;
		String string;

		public SequenceId() {
		}

		public SequenceId(Integer id, String string) {
			this.id = id;
			this.string = string;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getString() {
			return string;
		}

		public void setString(String string) {
			this.string = string;
		}

		@Override
		public String toString() {
			return id + ": " + string;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			SequenceId sequenceId = (SequenceId) o;
			return Objects.equals( string, sequenceId.string );
		}

		@Override
		public int hashCode() {
			return Objects.hash( string );
		}
	}
}
