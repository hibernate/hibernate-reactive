/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.issue;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.reactive.BaseReactiveTest;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.atIndex;

@Timeout(value = 10, timeUnit = MINUTES)

public class JoinedSubclassInheritanceWithManyToOneTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( ItemInstance.class, ClothingItemInstance.class, ClothingItem.class, Item.class );
	}

	/**
	 * Test for issue <a href="https://github.com/hibernate/hibernate-reactive/issues/1598">hibernate-reactive#1598</a>
	 */
	@Test
	public void testSubclassListAll(VertxTestContext context) {
		ClothingItem clothingItem = new ClothingItem();
		clothingItem.name = "Clothing item";

		ClothingItemInstance itemInstance = new ClothingItemInstance();
		itemInstance.name = "Clothing item instance";
		itemInstance.item = clothingItem;

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( clothingItem, itemInstance ) )
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session
								.createQuery( "from ItemInstance", ItemInstance.class ).getResultList()
								.invoke( list -> assertThat( list ).hasSize( 1 )
										.satisfies( entry -> assertThat( entry )
														.isInstanceOf( ClothingItemInstance.class )
														.hasFieldOrPropertyWithValue( "name", itemInstance.name ),
												atIndex( 0 )
										) ) )
				)
		);
	}


	@Entity(name = "Item")
	@Table(name = "item")
	@Inheritance(strategy = InheritanceType.JOINED)
	public static abstract class Item {

		@Id
		@GeneratedValue
		public Long id;

		public String name;

		@Override
		public String toString() {
			return id + ":" + name;
		}
	}

	@Entity(name = "ClothingItem")
	@Table(name = "clothingitem")
	public static class ClothingItem extends Item {
	}


	@Entity(name = "ItemInstance")
	@Table(name = "iteminstance")
	@Inheritance(strategy = InheritanceType.JOINED)
	public static abstract class ItemInstance {

		@Id
		@GeneratedValue
		public Long id;

		public String name;

		@ManyToOne(fetch = FetchType.EAGER)
		public Item item;

		@Override
		public String toString() {
			return getClass() + ":" + id + ":" + name;
		}


		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			ItemInstance that = (ItemInstance) o;
			return Objects.equals( name, that.name ) && Objects.equals( item, that.item );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name, item );
		}
	}


	@Entity(name = "ClothingItemInstance")
	@Table(name = "clothingiteminstance")
	public static class ClothingItemInstance extends ItemInstance {

	}
}
