/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import io.vertx.ext.unit.TestContext;

import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.OptimisticLocking;

import org.junit.Test;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import static org.hibernate.annotations.OptimisticLockType.ALL;
import static org.hibernate.annotations.OptimisticLockType.DIRTY;

public class OptimisticLockingTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Mergeable1.class, Mergeable2.class );
	}

	@Test
	public void test1(TestContext context) {
		Mergeable1 m = new Mergeable1();
		m.name = "Gavin";
		m.count = 69;
		test(
				context,
				getMutinySessionFactory()
						.withTransaction( (s, t) -> s.persist( m ) )
						.chain( v -> {
							m.name = "Davide";
							return getMutinySessionFactory()
									.withTransaction( (s, t) -> s.merge( m ) );
						} )
						.chain( v -> getMutinySessionFactory()
								.withSession( s -> s.find( Mergeable1.class, m.id ) )
								.invoke( mm -> context.assertEquals( mm.name, m.name ) )
						)
		);
	}

	@Test
	public void test2(TestContext context) {
		Mergeable2 m = new Mergeable2();
		m.name = "Gavin";
		m.count = 69;
		test(
				context,
				getMutinySessionFactory()
						.withTransaction( (s, t) -> s.persist( m ) )
						.chain( v -> {
							m.name = "Davide";
							return getMutinySessionFactory()
									.withTransaction( (s, t) -> s.merge( m ) );
						} )
						.chain( v -> getMutinySessionFactory()
								.withSession( s -> s.find( Mergeable2.class, m.id ) )
								.invoke( mm -> context.assertEquals( mm.name, m.name ) )
						)
		);

	}

	@Entity(name = "Mergeable1")
	@DynamicUpdate
	@OptimisticLocking(type = DIRTY)
	static class Mergeable1 {
		@Id
		@GeneratedValue
		Long id;
		String name;
		int count;
	}

	@Entity(name = "Mergeable2")
	@DynamicUpdate
	@OptimisticLocking(type = ALL)
	static class Mergeable2 {
		@Id
		@GeneratedValue
		Long id;
		String name;
		int count;
	}
}
