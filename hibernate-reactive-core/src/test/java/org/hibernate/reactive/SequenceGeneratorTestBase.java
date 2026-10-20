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
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

public abstract class SequenceGeneratorTestBase extends BaseReactiveTest {

	@Timeout(value = 10, timeUnit = MINUTES)
	public static class DefaultSchemaTest extends SequenceGeneratorTestBase {
		@Override
		protected Configuration constructConfiguration() {
			// Keep the default configuration
			return super.constructConfiguration();
		}
	}

	@Timeout(value = 10, timeUnit = MINUTES)
	public static class CustomSchemaTest extends SequenceGeneratorTestBase {

		@Override
		protected Configuration constructConfiguration() {
			Configuration configuration = super.constructConfiguration();
			configuration.setProperty( Settings.DEFAULT_SCHEMA, schemaName() );
			configuration.setProperty( Settings.HBM2DDL_CREATE_SCHEMAS, "true" );
			return configuration;
		}

		/**
		 * Oracle Database automatically creates a schema when you create a user and we don't have
		 * enough permission to create a new one in the container, for the other databases
		 * we test with a schema that doesn't match the username.
		 */
		private String schemaName() {
			return dbType() == ORACLE
					? DatabaseConfiguration.USERNAME
					: "hrSequenceGenerator";
		}
	}

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

	@Entity(name = "SequenceId")
	@SequenceGenerator(name = "seq", sequenceName = "test_id_seq", initialValue = 5, allocationSize = 1)
	@Table(name = SequenceId.TABLE)
	public static class SequenceId {
		public static final String TABLE = "SGTB_SequenceId";

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
