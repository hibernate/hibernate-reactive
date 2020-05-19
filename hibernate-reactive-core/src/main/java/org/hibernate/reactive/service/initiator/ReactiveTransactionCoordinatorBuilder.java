package org.hibernate.reactive.service.initiator;

import org.hibernate.resource.jdbc.spi.PhysicalConnectionHandlingMode;
import org.hibernate.resource.transaction.spi.TransactionCoordinator;
import org.hibernate.resource.transaction.spi.TransactionCoordinatorBuilder;
import org.hibernate.resource.transaction.spi.TransactionCoordinatorOwner;

/**
 * An implementation of the Hibernate {@link TransactionCoordinatorBuilder}
 * service for Hibernate Reactive.
 */
public class ReactiveTransactionCoordinatorBuilder implements TransactionCoordinatorBuilder {

	private final TransactionCoordinatorBuilder delegate;

	public ReactiveTransactionCoordinatorBuilder(TransactionCoordinatorBuilder defaultBuilder) {
		this.delegate = defaultBuilder;
	}

	@Override
	public TransactionCoordinator buildTransactionCoordinator(TransactionCoordinatorOwner owner, Options options) {
		return delegate.buildTransactionCoordinator( owner, options );
	}

	@Override
	public boolean isJta() {
		return delegate.isJta();
	}

	@Override
	public PhysicalConnectionHandlingMode getDefaultConnectionHandlingMode() {
		return delegate.getDefaultConnectionHandlingMode();
	}
}
