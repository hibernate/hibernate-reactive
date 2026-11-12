/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider;

import java.util.Map;

import org.hibernate.jpa.HibernatePersistenceProvider;
import org.hibernate.reactive.provider.internal.ReactiveProviderChecker;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The persistence provider can be selected using either the Jakarta or the legacy javax setting
 */
public class ReactiveProviderCheckerTest {

	private static final String REACTIVE_PROVIDER = ReactivePersistenceProvider.class.getName();

	@Test
	public void jakartaPersistenceProvider() {
		Map<String, String> integration = Map.of( Settings.JAKARTA_PERSISTENCE_PROVIDER, REACTIVE_PROVIDER );
		assertThat( ReactiveProviderChecker.extractRequestedProviderName( null, integration ) )
				.isEqualTo( REACTIVE_PROVIDER );
	}

	@Test
	@SuppressWarnings("deprecation")
	public void legacyPersistenceProvider() {
		Map<String, String> integration = Map.of( Settings.JPA_PERSISTENCE_PROVIDER, REACTIVE_PROVIDER );
		assertThat( ReactiveProviderChecker.extractRequestedProviderName( null, integration ) )
				.isEqualTo( REACTIVE_PROVIDER );
	}

	@Test
	@SuppressWarnings("deprecation")
	public void jakartaPersistenceProviderTakesPrecedence() {
		Map<String, String> integration = Map.of(
				Settings.JAKARTA_PERSISTENCE_PROVIDER, REACTIVE_PROVIDER,
				Settings.JPA_PERSISTENCE_PROVIDER, HibernatePersistenceProvider.class.getName()
		);
		assertThat( ReactiveProviderChecker.extractRequestedProviderName( null, integration ) )
				.isEqualTo( REACTIVE_PROVIDER );
	}
}
