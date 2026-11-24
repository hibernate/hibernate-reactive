/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.timezones;

import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.annotations.EnabledFor;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import jakarta.persistence.Tuple;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.cfg.AvailableSettings.JDBC_TIME_ZONE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

/**
 * Like {@link JDBCTimeZoneZonedTest}, but for the types stored in a time column:
 * the time of day is stored in the time zone of {@value AvailableSettings#JDBC_TIME_ZONE}.
 */
@Timeout(value = 10, timeUnit = MINUTES)
@EnabledFor(value = { POSTGRESQL, COCKROACHDB, MYSQL, MARIA }, reason = "The test reads the stored time with str(), which returns HH:mm:ss on these databases")
public class JDBCTimeZoneTimeTest extends BaseReactiveTest {

	private static final String JDBC_ZONE = "GMT+5";

	private static final LocalTime LOCAL_TIME = LocalTime.of( 12, 0 );

	// A time is bound as a java.sql.Time on January 1, 1970 in the time zone of the JVM
	private static final LocalTime STORED_TIME = LOCAL_TIME.atDate( LocalDate.EPOCH )
			.atZone( ZoneId.systemDefault() )
			.withZoneSameInstant( ZoneId.of( JDBC_ZONE ) )
			.toLocalTime();

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( TimeEntity.class );
	}

	@Override
	protected void setProperties(Configuration configuration) {
		super.setProperties( configuration );
		configuration.setProperty( JDBC_TIME_ZONE, JDBC_ZONE );
		configuration.setProperty( AvailableSettings.JAVA_TIME_USE_DIRECT_JDBC, "false" );
	}

	@Test
	public void test(VertxTestContext context) {
		final Time lunch = Time.valueOf( LOCAL_TIME );
		final TimeEntity entity = new TimeEntity( 1L, LOCAL_TIME, lunch, new Date( lunch.getTime() ) );
		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( entity ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> s.find( TimeEntity.class, entity.id ) ) )
				.thenAccept( found -> assertThat( found ).isEqualTo( entity ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> s
						.createSelectionQuery( "select str(wakeUp), str(lunch), str(dinner) from TimeEntity", Tuple.class )
						.getSingleResult() ) )
				.thenAccept( stored -> {
					final String expected = STORED_TIME.format( ISO_LOCAL_TIME );
					assertThat( stored.get( 0, String.class ) ).startsWith( expected );
					assertThat( stored.get( 1, String.class ) ).startsWith( expected );
					assertThat( stored.get( 2, String.class ) ).startsWith( expected );
				} )
		);
	}

	@Entity(name = "TimeEntity")
	public static class TimeEntity {
		@Id
		Long id;

		LocalTime wakeUp;

		Time lunch;

		@Temporal(TemporalType.TIME)
		Date dinner;

		public TimeEntity() {
		}

		public TimeEntity(Long id, LocalTime wakeUp, Time lunch, Date dinner) {
			this.id = id;
			this.wakeUp = wakeUp;
			this.lunch = lunch;
			this.dinner = dinner;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			TimeEntity that = (TimeEntity) o;
			// Compare the milliseconds: dinner is read as a java.sql.Time
			return Objects.equals( wakeUp, that.wakeUp )
					&& Objects.equals( lunch, that.lunch )
					&& dinner.getTime() == that.dinner.getTime();
		}

		@Override
		public int hashCode() {
			return Objects.hash( wakeUp, lunch, dinner.getTime() );
		}

		@Override
		public String toString() {
			return id + ": " + wakeUp + ", " + lunch + ", " + dinner;
		}
	}
}
