/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

/**
 * Same as {@link RollbackOnlyTest}, but with JPA transaction compliance enabled:
 * like in Hibernate ORM, committing a transaction marked for rollback only
 * fails with a {@link jakarta.persistence.RollbackException}.
 */
public class RollbackOnlyJpaComplianceTest extends RollbackOnlyTest {

	@Override
	protected boolean jpaTransactionCompliance() {
		return true;
	}
}
