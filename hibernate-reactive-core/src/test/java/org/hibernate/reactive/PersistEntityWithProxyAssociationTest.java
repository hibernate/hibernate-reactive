/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.annotations.NaturalId;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Adapted from the test with the same name in Hibernate ORM.
 * <p>
 * Persist an entity with an association to a proxy, when the association
 * references a natural id instead of the primary key.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class PersistEntityWithProxyAssociationTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Position.class, Payment.class, Invoice.class, Receipt.class, Wallet.class, Currency.class );
	}

	@Test
	public void testPersistWithProxy(VertxTestContext context) {
		final Currency usd = new Currency( 1L, "USD" );
		final Holding holding = new Holding( 20, null );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( usd ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> {
					// The association references the code of the currency: Hibernate needs it to insert the holding
					holding.currency = s.getReference( Currency.class, usd.id );
					return s.persist( new Position( 10L, List.of( holding ) ) );
				} ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery( "select h.currency.code from Position p join p.holdings h", String.class )
						.getSingleResult() ) )
				.invoke( code -> assertThat( code ).isEqualTo( usd.code ) )
		);
	}

	@Test
	public void testUpdateCollectionWithProxy(VertxTestContext context) {
		final Currency usd = new Currency( 1L, "USD" );
		final Currency eur = new Currency( 2L, "EUR" );
		final Position position = new Position( 10L, List.of() );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( usd, eur ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> {
					position.holdings.add( new Holding( 20, s.getReference( Currency.class, usd.id ) ) );
					return s.persist( position )
							// Unlike testPersistWithProxy, the second holding updates a collection already in the database
							.call( s::flush )
							.invoke( () -> position.holdings
									.add( new Holding( 30, s.getReference( Currency.class, eur.id ) ) ) );
				} ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery( "select h.currency.code from Position p join p.holdings h order by h.quantity", String.class )
						.getResultList() ) )
				.invoke( codes -> assertThat( codes ).containsExactly( usd.code, eur.code ) )
		);
	}

	@Test
	public void testPersistEntityWithProxy(VertxTestContext context) {
		final Currency eur = new Currency( 2L, "EUR" );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( eur ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.persist( new Payment( 30L, s.getReference( Currency.class, eur.id ) ) ) ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery( "select p.currency.code from Payment p", String.class )
						.getSingleResult() ) )
				.invoke( code -> assertThat( code ).isEqualTo( eur.code ) )
		);
	}

	@Test
	public void testPersistEntityWithProxyAndNullableColumn(VertxTestContext context) {
		final Currency eur = new Currency( 2L, "EUR" );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( eur ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.persist( new Invoice( 40L, s.getReference( Currency.class, eur.id ) ) ) ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery( "select i.currency.code from Invoice i", String.class )
						.getSingleResultOrNull() ) )
				.invoke( code -> assertThat( code ).isEqualTo( eur.code ) )
		);
	}

	@Test
	public void testUpdateEntityWithProxy(VertxTestContext context) {
		final Currency usd = new Currency( 1L, "USD" );
		final Currency eur = new Currency( 2L, "EUR" );
		final Invoice invoice = new Invoice( 40L, usd );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( usd, eur, invoice ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.find( Invoice.class, invoice.id )
						.invoke( found -> found.currency = s.getReference( Currency.class, eur.id ) ) ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery( "select i.currency.code from Invoice i", String.class )
						.getSingleResultOrNull() ) )
				.invoke( code -> assertThat( code ).isEqualTo( eur.code ) )
		);
	}

	@Test
	public void testPersistEntityWithIdentityAndProxy(VertxTestContext context) {
		final Currency eur = new Currency( 2L, "EUR" );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( eur ) )
				// With an identity column, the insert happens during persist() and not during the flush
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.persist( new Receipt( s.getReference( Currency.class, eur.id ) ) ) ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery( "select r.currency.code from Receipt r", String.class )
						.getSingleResultOrNull() ) )
				.invoke( code -> assertThat( code ).isEqualTo( eur.code ) )
		);
	}

	@Test
	public void testStatelessInsertWithProxy(VertxTestContext context) {
		final Currency eur = new Currency( 2L, "EUR" );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( eur ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> Uni.createFrom().item( s.getReference( Currency.class, eur.id ) ) ) )
				.chain( proxy -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.insert( new Invoice( 40L, proxy ) ) ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery( "select i.currency.code from Invoice i", String.class )
						.getSingleResultOrNull() ) )
				.invoke( code -> assertThat( code ).isEqualTo( eur.code ) )
		);
	}

	@Test
	public void testStatelessUpdateWithProxy(VertxTestContext context) {
		final Currency usd = new Currency( 1L, "USD" );
		final Currency eur = new Currency( 2L, "EUR" );
		final Invoice invoice = new Invoice( 40L, usd );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( usd, eur, invoice ) )
				.chain( () -> getMutinySessionFactory().withSession( s -> Uni.createFrom().item( s.getReference( Currency.class, eur.id ) ) ) )
				.chain( proxy -> getMutinySessionFactory().withStatelessTransaction( s -> {
					invoice.currency = proxy;
					return s.update( invoice );
				} ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery( "select i.currency.code from Invoice i", String.class )
						.getSingleResultOrNull() ) )
				.invoke( code -> assertThat( code ).isEqualTo( eur.code ) )
		);
	}

	@Test
	public void testManyToManyWithProxy(VertxTestContext context) {
		final Currency eur = new Currency( 2L, "EUR" );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( eur ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.persist( new Wallet( 50L, List.of( s.getReference( Currency.class, eur.id ) ) ) ) ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery( "select c.code from Wallet w join w.currencies c", String.class )
						.getSingleResultOrNull() ) )
				.invoke( code -> assertThat( code ).isEqualTo( eur.code ) )
		);
	}

	@Entity(name = "Receipt")
	@Table(name = "RECEIPT_TABLE")
	public static class Receipt {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		Long id;

		@ManyToOne(fetch = FetchType.LAZY)
		@JoinColumn(referencedColumnName = "code")
		Currency currency;

		public Receipt() {
		}

		public Receipt(Currency currency) {
			this.currency = currency;
		}
	}

	@Entity(name = "Wallet")
	@Table(name = "WALLET_TABLE")
	public static class Wallet {
		@Id
		Long id;

		@ManyToMany
		@JoinTable(name = "WALLET_CURRENCY", inverseJoinColumns = @JoinColumn(referencedColumnName = "code"))
		List<Currency> currencies = new ArrayList<>();

		public Wallet() {
		}

		public Wallet(Long id, List<Currency> currencies) {
			this.id = id;
			this.currencies = new ArrayList<>( currencies );
		}
	}

	@Entity(name = "Payment")
	@Table(name = "PAYMENT_TABLE")
	public static class Payment {
		@Id
		Long id;

		@ManyToOne(optional = false, fetch = FetchType.LAZY)
		@JoinColumn(referencedColumnName = "code", nullable = false)
		Currency currency;

		public Payment() {
		}

		public Payment(Long id, Currency currency) {
			this.id = id;
			this.currency = currency;
		}
	}

	@Entity(name = "Invoice")
	@Table(name = "INVOICE_TABLE")
	public static class Invoice {
		@Id
		Long id;

		@ManyToOne(fetch = FetchType.LAZY)
		@JoinColumn(referencedColumnName = "code")
		Currency currency;

		public Invoice() {
		}

		public Invoice(Long id, Currency currency) {
			this.id = id;
			this.currency = currency;
		}
	}

	@Entity(name = "Currency")
	@Table(name = "CURRENCY_TABLE")
	public static class Currency {
		@Id
		Long id;

		@NaturalId
		String code;

		public Currency() {
		}

		public Currency(Long id, String code) {
			this.id = id;
			this.code = code;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			return Objects.equals( code, ( (Currency) o ).code );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( code );
		}
	}

	@Entity(name = "Position")
	@Table(name = "POSITION_TABLE")
	public static class Position {
		@Id
		Long id;

		boolean active;

		@ElementCollection
		@JoinTable(name = "POSITION_HOLDING", joinColumns = @JoinColumn(name = "POSITION_ID"))
		List<Holding> holdings = new ArrayList<>();

		public Position() {
		}

		public Position(Long id, List<Holding> holdings) {
			this.id = id;
			this.holdings = new ArrayList<>( holdings );
		}
	}

	@Embeddable
	public static class Holding {
		@Column(nullable = false)
		Integer quantity;

		@ManyToOne(optional = false, fetch = FetchType.LAZY)
		@JoinColumn(referencedColumnName = "code", nullable = false)
		Currency currency;

		public Holding() {
		}

		public Holding(Integer quantity, Currency currency) {
			this.quantity = quantity;
			this.currency = currency;
		}
	}
}
