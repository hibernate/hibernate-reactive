/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

import org.hibernate.annotations.SourceType;
import org.hibernate.reactive.TimestampTest.DbEvent;
import org.hibernate.reactive.TimestampTest.DbRecord;
import org.hibernate.reactive.TimestampTest.Timestampable;
import org.hibernate.reactive.TimestampTest.VmEvent;
import org.hibernate.reactive.TimestampTest.VmRecord;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.hibernate.annotations.SourceType.VM;

/**
 * Like {@link TimestampTest}, but Hibernate ORM generates the timestamps using the default clock.
 * <p>
 * We cannot predict the generated values, and when the source is the VM each property reads the clock separately.
 * So we only check that the timestamps are close to each other and to the current time.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class TimestampWithDefaultClockTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( VmRecord.class, DbRecord.class, VmEvent.class, DbEvent.class );
	}

	@ParameterizedTest(name = "{0} source type generation, embedded: {2}")
	@MethodSource("org.hibernate.reactive.TimestampTest#records")
	public void shouldGenerateTimestampsForEntity(SourceType eventSourceType, Timestampable entity, boolean embedded, VertxTestContext context) {
		final Instant[] creationTime = { null };
		entity.setText( "initial text" );
		test( context, getMutinySessionFactory()
					  .withSession( session -> session
							  .persist( entity )
							  .chain( session::flush )
							  .invoke( () -> {
								  assertThat( entity.getCreated() )
										  .as( "At creation time, timestamps should be close and not null" )
										  .isNotNull()
										  .isCloseTo( entity.getUpdated(), within( 1, SECONDS ) )
										  .isCloseTo( entity.getCurrentTimestampInsert(), within( 1, SECONDS ) );
								  if ( eventSourceType == VM ) {
									  assertThat( entity.getCreated() ).isCloseTo( Instant.now(), within( 60, SECONDS ) );
								  }
								  assertThat( entity.getCurrentTimestampUpdate() ).isNull();
								  assertThat( entity.getCurrentTimestampForce() ).isNull();
								  assertThat( entity.getVersion() ).isZero();
								  creationTime[0] = entity.getCreated();
							  } )
							  .invoke( () -> entity.setText( "edited text" ) )
							  .chain( session::flush )
							  .invoke( () -> {
								  assertThat( entity ).extracting( Timestampable::getVersion ).isEqualTo( 1L );
								  assertThat( entity.getCurrentTimestampUpdate() )
										  .as( "Update times should be close and not null" )
										  .isNotNull()
										  .isCloseTo( entity.getUpdated(), within( 1, SECONDS ) );
								  assertThat( entity.getCreated() )
										  .as( "Creation time should not change after update and should not be after the update time" )
										  .isEqualTo( creationTime[0] )
										  .isBeforeOrEqualTo( entity.getUpdated() );
								  assertThat( entity.getCurrentTimestampForce() ).isNull();
							  } )
					  )
				// Check that the values have been saved correctly
				.call( () -> getMutinySessionFactory().withTransaction( session -> session
						.find( entity.getClass(), entity.getId() ) )
						.map( Timestampable.class::cast )
						.invoke( result -> {
							assertThat( result ).isNotNull();
							assertThat( result.getCreated() ).isEqualTo( entity.getCreated() );
							assertThat( result.getUpdated() ).isEqualTo( entity.getUpdated() );
							assertThat( result.getCurrentTimestampInsert() ).isEqualTo( entity.getCurrentTimestampInsert() );
							assertThat( result.getCurrentTimestampUpdate() ).isEqualTo( entity.getCurrentTimestampUpdate() );
							assertThat( result.getCurrentTimestampForce() ).isEqualTo( entity.getCurrentTimestampForce() );
						} )
				)
		);
	}
}
