package org.hibernate.rx.service.initiator;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.registry.selector.spi.StrategySelector;
import org.hibernate.resource.transaction.internal.TransactionCoordinatorBuilderInitiator;
import org.hibernate.resource.transaction.spi.TransactionCoordinatorBuilder;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * integrates our implementation of {@link TransactionCoordinatorBuilder}.
 *
 * @see TransactionCoordinatorBuilder
 */
public class RxTransactionCoordinatorBuilderInitiator implements StandardServiceInitiator<TransactionCoordinatorBuilder> {

	public static final RxTransactionCoordinatorBuilderInitiator INSTANCE = new RxTransactionCoordinatorBuilderInitiator();

	private RxTransactionCoordinatorBuilderInitiator() {
	}

	@Override
	public Class<TransactionCoordinatorBuilder> getServiceInitiated() {
		return TransactionCoordinatorBuilder.class;
	}

	@Override
	public TransactionCoordinatorBuilder initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		TransactionCoordinatorBuilder defaultBuilder = TransactionCoordinatorBuilderInitiator.INSTANCE.initiateService( configurationValues, registry );
		return new RxTransactionCoordinatorBuilder( defaultBuilder );
	}


	private TransactionCoordinatorBuilder getDefaultBuilder(ServiceRegistryImplementor registry, String strategy ) {
		return registry.getService( StrategySelector.class ).resolveStrategy( TransactionCoordinatorBuilder.class, strategy );
	}
}
