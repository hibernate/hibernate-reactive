package org.hibernate.rx.persister.entity.impl;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface RxAccessCallback {
	/**
	 * Retrieve the next value from the underlying source.
	 *
	 * @return The next value.
	 */
	CompletionStage<Optional<Long>> getNextValue();

	/**
	 * Obtain the tenant identifier (multi-tenancy), if one, associated with this callback.
	 *
	 * @return The tenant identifier
	 */
	String getTenantIdentifier();
}

