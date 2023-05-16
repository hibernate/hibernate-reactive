/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.mutiny.impl.MutinySessionImpl;
import org.hibernate.reactive.pool.BatchingConnection;
import org.hibernate.reactive.pool.impl.SqlClientConnection;
import org.hibernate.reactive.stage.impl.StageSessionImpl;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * We test that the right implementation of {@link org.hibernate.reactive.pool.ReactiveConnection} is used
 * when we set {@link org.hibernate.reactive.mutiny.Mutiny.Session#setBatchSize(Integer)}
 */
public class BatchSizeTest extends BaseReactiveTest {

	@Test
	public void testSetBatchSize(VertxTestContext context) {
		test( context, openSession()
				.thenAccept( session -> {
					assertThat( ( (StageSessionImpl) session ).getReactiveConnection() ).isInstanceOf( SqlClientConnection.class );
					session.setBatchSize( 200 );
					assertThat( ( (StageSessionImpl) session ).getReactiveConnection() ).isInstanceOf( BatchingConnection.class );
				} )
		);
	}

	@Test
	public void testSetBatchSizeMutiny(VertxTestContext context) {
		test( context, openMutinySession()
				.invoke( session -> {
					assertThat( ( (MutinySessionImpl) session ).getReactiveConnection() ).isInstanceOf( SqlClientConnection.class );
					session.setBatchSize( 200 );
					assertThat( ( (MutinySessionImpl) session ).getReactiveConnection() ).isInstanceOf( BatchingConnection.class );
				} )
		);
	}
}
