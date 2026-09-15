/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.FetchNotFoundException;
import org.hibernate.annotations.NotFound;
import org.hibernate.annotations.NotFoundAction;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;

@Timeout(value = 10, timeUnit = MINUTES)
public class NotFoundTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class, City.class, Coin.class, Currency.class );
	}

	@Override
	protected CompletionStage<Void> cleanDb() {
		// Loading Coin via the default cleanup would hit @NotFound(EXCEPTION)
		// after the broken-FK tests. Native deletes skip that path.
		return getMutinySessionFactory()
				.withTransaction( s -> s.createNativeQuery( "delete from NFCoin" ).executeUpdate()
						.call( () -> s.createNativeQuery( "delete from NFPerson" ).executeUpdate() )
						.call( () -> s.createNativeQuery( "delete from NFCurrency" ).executeUpdate() )
						.call( () -> s.createNativeQuery( "delete from NFCity" ).executeUpdate() ) )
				.replaceWithVoid()
				.subscribeAsCompletionStage();
	}

	@Test
	public void ignoreReturnsAssociationWhenTargetExists(VertxTestContext context) {
		final City city = new City( 1, "New York" );
		final Person person = new Person( 1, "John Doe", city );
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.persistAll( city, person ) )
						.chain( () -> getMutinySessionFactory().withTransaction( s -> s.find( Person.class, 1 ) ) )
						.invoke( loaded -> {
							assertThat( loaded ).isNotNull();
							assertThat( loaded.getName() ).isEqualTo( "John Doe" );
							assertThat( loaded.getCity() ).isNotNull();
							assertThat( loaded.getCity().getName() ).isEqualTo( "New York" );
						} )
		);
	}

	@Test
	public void ignoreReturnsNullWhenTargetIsMissing(VertxTestContext context) {
		final City city = new City( 2, "Paris" );
		final Person person = new Person( 2, "Jane Doe", city );
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.persistAll( city, person ) )
						.chain( () -> getMutinySessionFactory().withTransaction( s -> s
								.createNativeQuery( "delete from NFCity where id = 2" )
								.executeUpdate() ) )
						.chain( () -> getMutinySessionFactory().withTransaction( s -> s.find( Person.class, 2 ) ) )
						.invoke( loaded -> {
							assertThat( loaded ).isNotNull();
							assertThat( loaded.getName() ).isEqualTo( "Jane Doe" );
							assertThat( loaded.getCity() ).isNull();
						} )
		);
	}

	@Test
	public void exceptionReturnsAssociationWhenTargetExists(VertxTestContext context) {
		final Currency usd = new Currency( 1, "USD" );
		final Coin penny = new Coin( 1, "Penny", usd );
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.persistAll( usd, penny ) )
						.chain( () -> getMutinySessionFactory().withTransaction( s -> s.find( Coin.class, 1 ) ) )
						.invoke( loaded -> {
							assertThat( loaded ).isNotNull();
							assertThat( loaded.getName() ).isEqualTo( "Penny" );
							assertThat( loaded.getCurrency() ).isNotNull();
							assertThat( loaded.getCurrency().getName() ).isEqualTo( "USD" );
						} )
		);
	}

	@Test
	public void exceptionThrowsWhenTargetIsMissing(VertxTestContext context) {
		final Currency euro = new Currency( 2, "Euro" );
		final Coin fiveC = new Coin( 2, "Five cents", euro );
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.persistAll( euro, fiveC ) )
						.chain( () -> getMutinySessionFactory().withTransaction( s -> s
								.createNativeQuery( "delete from NFCurrency where id = 2" )
								.executeUpdate() ) )
						.chain( () -> assertThrown(
								FetchNotFoundException.class,
								getMutinySessionFactory().withTransaction( s -> s.find( Coin.class, 2 ) )
						) )
						.invoke( exception -> {
							assertThat( exception.getEntityName() ).isEqualTo( Currency.class.getName() );
							assertThat( exception.getIdentifier() ).isEqualTo( 2 );
						} )
		);
	}

	@Test
	public void exceptionThrowsWhenQueryingOwnerWithMissingTarget(VertxTestContext context) {
		final Currency yen = new Currency( 3, "Yen" );
		final Coin yenCoin = new Coin( 3, "Yen coin", yen );
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.persistAll( yen, yenCoin ) )
						.chain( () -> getMutinySessionFactory().withTransaction( s -> s
								.createNativeQuery( "delete from NFCurrency where id = 3" )
								.executeUpdate() ) )
						.chain( () -> assertThrown(
								FetchNotFoundException.class,
								getMutinySessionFactory().withTransaction( s -> s
										.createSelectionQuery( "from Coin c where c.id = 3", Coin.class )
										.getResultList() )
						) )
						.invoke( exception -> {
							assertThat( exception.getEntityName() ).isEqualTo( Currency.class.getName() );
							assertThat( exception.getIdentifier() ).isEqualTo( 3 );
						} )
		);
	}

	@Entity(name = "Person")
	@Table(name = Person.TABLE)
	public static class Person {
		public static final String TABLE = "NFPerson";

		@Id
		private Integer id;
		private String name;

		@ManyToOne
		@NotFound(action = NotFoundAction.IGNORE)
		@JoinColumn(name = "city_fk")
		private City city;

		public Person() {
		}

		public Person(Integer id, String name, City city) {
			this.id = id;
			this.name = name;
			this.city = city;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public City getCity() {
			return city;
		}
	}

	@Entity(name = "City")
	@Table(name = City.TABLE)
	public static class City {
		public static final String TABLE = "NFCity";

		@Id
		private Integer id;
		private String name;

		public City() {
		}

		public City(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}
	}

	@Entity(name = "Coin")
	@Table(name = Coin.TABLE)
	public static class Coin {
		public static final String TABLE = "NFCoin";

		@Id
		private Integer id;
		private String name;

		@ManyToOne(fetch = FetchType.EAGER)
		@NotFound(action = NotFoundAction.EXCEPTION)
		@JoinColumn(name = "currency_fk")
		private Currency currency;

		public Coin() {
		}

		public Coin(Integer id, String name, Currency currency) {
			this.id = id;
			this.name = name;
			this.currency = currency;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public Currency getCurrency() {
			return currency;
		}
	}

	@Entity(name = "Currency")
	@Table(name = Currency.TABLE)
	public static class Currency {
		public static final String TABLE = "NFCurrency";

		@Id
		private Integer id;
		private String name;

		public Currency() {
		}

		public Currency(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}
	}
}
