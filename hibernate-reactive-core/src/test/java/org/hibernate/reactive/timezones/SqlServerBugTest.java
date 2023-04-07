/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.timezones;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

public class SqlServerBugTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of(Thing.class);
	}

	@Test
	public void test(TestContext context) {
		test( context, getSessionFactory().withTransaction( s -> s.persist( new Thing() ) ) );
	}

	@Entity
	public static class Thing {
		@Id
		@GeneratedValue Long id;
	}
}
