/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.CurrentTimestamp;
import org.hibernate.annotations.SourceType;
import org.hibernate.annotations.UpdateTimestamp;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.annotations.SourceType.DB;
import static org.hibernate.annotations.SourceType.VM;
import static org.hibernate.generator.EventType.FORCE_INCREMENT;
import static org.hibernate.generator.EventType.INSERT;
import static org.hibernate.generator.EventType.UPDATE;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Timeout(value = 10, timeUnit = MINUTES)
public class TimestampTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( VmRecord.class, DbRecord.class, VmEvent.class, DbEvent.class );
	}

	static Stream<Arguments> records() {
		return Stream.of(
				// Entity
				arguments( VM, new VmRecord(), false ),
				arguments( DB, new DbRecord(), false ),
				// Embedded
				arguments( VM, new VmEvent(), true ),
				arguments( DB, new DbEvent(), true )
		);
	}

	@ParameterizedTest(name = "{0} source type generation, embedded: {2}")
	@MethodSource("records")
	public void shouldGenerateTimestampsForEntity(SourceType eventSourceType, Timestampable entity, boolean embedded, VertxTestContext context) {
		final Instant[] creationTime = { null };
		entity.setText( "initial text" );
		test( context, getMutinySessionFactory()
					  .withSession( session -> session
							  .persist( entity )
							  .chain( session::flush )
							  .invoke( () -> {
								  assertThat( entity.getCreated().truncatedTo( SECONDS ) )
										  .as( "At creation time, timestamps should be equal and not null" )
										  .isNotNull()
										  .isEqualTo( entity.getUpdated().truncatedTo( SECONDS ) )
										  .isEqualTo( entity.getCurrentTimestampInsert().truncatedTo( SECONDS ) );
								  if ( embedded && eventSourceType == DB) {
									  assertThat( entity.getCreated().truncatedTo( SECONDS ) )
											  .isEqualTo( entity.getCurrentTimestampUpdate().truncatedTo( SECONDS ) );
								  }
								  else {
									  assertThat( entity.getCurrentTimestampUpdate() ).isNull();
								  }
								  assertThat( entity.getCurrentTimestampForce() ).isNull();
								  assertThat( entity.getVersion() ).isZero();
								  creationTime[0] = entity.getCreated();
							  } )
							  .invoke( () -> entity.setText( "edited text" ) )
							  .chain( session::flush )
							  .invoke( () -> {
								  assertThat( entity ).extracting( Timestampable::getVersion ).isEqualTo( 1L );
								  assertThat( entity.getCurrentTimestampUpdate() )
										  .as( "Update time should not be null" )
										  .isNotNull();
								  assertThat( entity.getCurrentTimestampUpdate().truncatedTo(  SECONDS ) )
										  .as(  "Update times should be equal and not null" )
										  .isEqualTo( entity.getUpdated().truncatedTo( SECONDS ) );
								  assertThat( entity.getCreated().truncatedTo( SECONDS ) )
										  .as( "Creation time should not change after update and should be before the update time" )
										  .isEqualTo( creationTime[0].truncatedTo( SECONDS ) )
										  // They can still be equal if the testsuite is fast enough
										  .isEqualTo( entity.getCurrentTimestampInsert().truncatedTo( SECONDS ) )
										  .isBeforeOrEqualTo( entity.getCurrentTimestampUpdate().truncatedTo( SECONDS ) )
										  .isBeforeOrEqualTo( entity.getUpdated().truncatedTo( SECONDS ) );
								  // This doesn't seem right for entities, embedded value don't have a @Version column
								  assertThat( entity.getCurrentTimestampForce() ).isNull();
							  } )
					  )
				// Maybe it's overkill, but let's test values have been saved correctly
				.call( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( entity.getClass(), entity.getId() ) )
						.map( Timestampable.class::cast )
						.invoke( result -> {
							assertThat( result ).isNotNull();
							assertThat( result.getCreated() ).isEqualTo( entity.getCreated() );
							assertThat( result.getUpdated() ).isEqualTo( entity.getUpdated() );
							assertThat( result.getCurrentTimestampUpdate() ).isEqualTo( entity.getCurrentTimestampUpdate() );
							assertThat( result.getCurrentTimestampForce() ).isEqualTo( entity.getCurrentTimestampForce() );
							assertThat( result.getCurrentTimestampUpdate() ).isEqualTo( entity.getCurrentTimestampUpdate() );
						} )
				)
		);
	}

	/**
	 * So that we can parameterize the test for different event sources
	 */
	public interface Timestampable {
		Object getId();
		long getVersion();
		void setText(String text);
		Instant getCreated();
		Instant getUpdated();
		Instant getCurrentTimestampUpdate();
		Instant getCurrentTimestampInsert();

		// Embeddable don't support this
		default Instant getCurrentTimestampForce() {
			return null;
		};
	}

	@Entity
	@Table(name = VmRecord.TABLE_NAME)
	static class VmRecord implements Timestampable {
		public static final String TABLE_NAME = "VM_RECORD_TIMESTAMP_TEST";
		@Id
		@GeneratedValue
		Long id;

		@Version
		long version;

		String text;

		@CreationTimestamp(source = VM)
		Instant created;
		@UpdateTimestamp(source = VM)
		Instant updated;

		@CurrentTimestamp(event = INSERT, source = VM)
		Instant currentTimestampInsert;
		@CurrentTimestamp(event = UPDATE, source = VM)
		Instant currentTimestampUpdate;
		@CurrentTimestamp(event = FORCE_INCREMENT, source = VM)
		Instant currentTimestampForce;

		@Override
		public Long getId() {
			return id;
		}

		@Override
		public void setText(String text) {
			this.text = text;
		}

		@Override
		public long getVersion() {
			return version;
		}

		@Override
		public Instant getCreated() {
			return created;
		}

		@Override
		public Instant getUpdated() {
			return updated;
		}

		@Override
		public Instant getCurrentTimestampUpdate() {
			return currentTimestampUpdate;
		}

		@Override
		public Instant getCurrentTimestampInsert() {
			return currentTimestampInsert;
		}

		@Override
		public Instant getCurrentTimestampForce() {
			return currentTimestampForce;
		}
	}

	@Entity
	@Table(name = DbRecord.TABLE_NAME)
	static class DbRecord implements Timestampable {
		public static final String TABLE_NAME = "DB_RECORD_TIMESTAMP_TEST";

		@Id
		@GeneratedValue
		Long id;

		@Version
		long version;

		String text;

		@CreationTimestamp(source = DB)
		Instant created;
		@UpdateTimestamp(source = DB)
		Instant updated;

		@CurrentTimestamp(event = INSERT, source = DB)
		Instant currentTimestampInsert;
		@CurrentTimestamp(event = UPDATE, source = DB)
		Instant currentTimestampUpdate;
		@CurrentTimestamp(event = FORCE_INCREMENT, source = DB)
		Instant currentTimestampForce;

		@Override
		public Long getId() {
			return id;
		}

		@Override
		public void setText(String text) {
			this.text = text;
		}

		@Override
		public long getVersion() {
			return version;
		}

		@Override
		public Instant getCreated() {
			return created;
		}

		@Override
		public Instant getUpdated() {
			return updated;
		}

		@Override
		public Instant getCurrentTimestampUpdate() {
			return currentTimestampUpdate;
		}

		@Override
		public Instant getCurrentTimestampInsert() {
			return currentTimestampInsert;
		}

		@Override
		public Instant getCurrentTimestampForce() {
			return currentTimestampForce;
		}
	}

	@Entity
	static class DbEvent implements Timestampable {

		@Id
		@GeneratedValue
		public Long id;

		@Version
		long version;

		public String text;

		@Embedded
		public DbEmbedded dbEmbedded;

		@Override
		public Object getId() {
			return id;
		}

		@Override
		public long getVersion() {
			return version;
		}

		@Override
		public void setText(String text) {
			this.text = text;
		}

		@Override
		public Instant getCreated() {
			return dbEmbedded.created;
		}

		@Override
		public Instant getUpdated() {
			return dbEmbedded.updated;
		}

		@Override
		public Instant getCurrentTimestampUpdate() {
			return dbEmbedded.currentTimestampUpdate;
		}

		@Override
		public Instant getCurrentTimestampInsert() {
			return dbEmbedded.currentTimestampInsert;
		}
	}

	@Entity
	static class VmEvent implements Timestampable {

		@Id
		@GeneratedValue
		public Long id;

		@Version
		long version;

		public String text;

		@Embedded
		public VmEmbedded vmEmbedded;

		@Override
		public Object getId() {
			return id;
		}

		@Override
		public long getVersion() {
			return version;
		}

		@Override
		public void setText(String text) {
			this.text = text;
		}

		@Override
		public Instant getCreated() {
			return vmEmbedded.created;
		}

		@Override
		public Instant getUpdated() {
			return vmEmbedded.updated;
		}

		@Override
		public Instant getCurrentTimestampUpdate() {
			return vmEmbedded.currentTimestampUpdate;
		}

		@Override
		public Instant getCurrentTimestampInsert() {
			return vmEmbedded.currentTimestampInsert;
		}
	}

	@Embeddable
	static class DbEmbedded {
		@CreationTimestamp(source = DB)
		public Instant created;
		@UpdateTimestamp(source = DB)
		public Instant updated;

		@CurrentTimestamp(event = INSERT, source = DB)
		Instant currentTimestampInsert;
		@CurrentTimestamp(event = UPDATE, source = DB)
		Instant currentTimestampUpdate;
	}

	@Embeddable
	static class VmEmbedded {
		@CreationTimestamp(source = VM)
		Instant created;
		@UpdateTimestamp(source = VM)
		Instant updated;

		@CurrentTimestamp(event = INSERT, source = VM)
		Instant currentTimestampInsert;
		@CurrentTimestamp(event = UPDATE, source = VM)
		Instant currentTimestampUpdate;
	}
}
