/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)
public class EmbeddedIdTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return Set.of( Delivery.class );
	}

	LocationId verbania = new LocationId( "Italy", "Verbania" );
	Delivery pizza = new Delivery( verbania, "Pizza Margherita" );

	LocationId hallein = new LocationId( "Austria", "Hallein" );
	Delivery schnitzel = new Delivery( hallein, "Wiener Schnitzel" );

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction( s -> s.persistAll( pizza, schnitzel ) ) );
	}

	@Test
	public void testFindSingleId(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction( s -> s.find( Delivery.class, verbania ) )
				.invoke( result -> assertThat( result ).isEqualTo( pizza ) )
		);
	}

	@Test
	public void testFindMultipleIds(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction( s -> s.find( Delivery.class, verbania, hallein ) )
				.invoke( result -> assertThat( result ).containsExactlyInAnyOrder( pizza, schnitzel ) )
		);
	}

	@Entity(name = "Delivery")
	@Table(name = "Delivery")
	public static class Delivery {

		@EmbeddedId
		private LocationId locationId;

		@Column(name = "field")
		private String field;

		public Delivery() {
		}

		public Delivery(LocationId locationId, String field) {
			this.locationId = locationId;
			this.field = field;
		}

		public LocationId getLocationId() {
			return locationId;
		}

		public void setLocationId(LocationId locationId) {
			this.locationId = locationId;
		}

		public String getField() {
			return field;
		}

		public void setField(String field) {
			this.field = field;
		}

		@Override
		public String toString() {
			return locationId + ":" + field;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Delivery table = (Delivery) o;
			return Objects.equals( locationId, table.locationId ) &&
					Objects.equals( field, table.field );
		}

		@Override
		public int hashCode() {
			return Objects.hash( locationId, field );
		}
	}


	@Embeddable
	public static class LocationId {

		@Column(name = "sp_country")
		private String country;

		@Column(name = "sp_city")
		private String city;

		public LocationId(String country, String city) {
			this.country = country;
			this.city = city;
		}

		public LocationId() {
		}

		public String getCountry() {
			return country;
		}

		public String getCity() {
			return city;
		}

		public void setCountry(String country) {
			this.country = country;
		}

		public void setCity(String city) {
			this.city = city;
		}

		@Override
		public String toString() {
			return "[" + country + "-" + city + "]";
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			LocationId tableId = (LocationId) o;
			return Objects.equals( country, tableId.country ) &&
					Objects.equals( city, tableId.city );
		}

		@Override
		public int hashCode() {
			return Objects.hash( country, city );
		}
	}
}
