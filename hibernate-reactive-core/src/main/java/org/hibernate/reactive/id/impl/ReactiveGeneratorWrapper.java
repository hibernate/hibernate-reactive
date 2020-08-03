/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.ExportableProducer;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * @author Gavin King
 */
public class ReactiveGeneratorWrapper<T> implements
		IdentifierGenerator, ExportableProducer, ReactiveIdentifierGenerator<T> {

	private ReactiveIdentifierGenerator<T> reactiveGenerator;
	private IdentifierGenerator generator;

	public ReactiveGeneratorWrapper(ReactiveIdentifierGenerator<T> reactiveGenerator, IdentifierGenerator generator) {
		this.reactiveGenerator = reactiveGenerator;
		this.generator = generator;
	}

	public ReactiveGeneratorWrapper(ReactiveIdentifierGenerator<T> reactiveGenerator) {
		this.reactiveGenerator = reactiveGenerator;
	}

	@Override
	public CompletionStage<T> generate(ReactiveConnectionSupplier session, Object entity) {
		return reactiveGenerator.generate(session, entity);
	}

	@Override
	public Serializable generate(SharedSessionContractImplementor session, Object object) {
		if (generator == null) {
			throw new UnsupportedOperationException("reactive generator");
		}
		return generator.generate( session, object );
	}

	@Override
	public void registerExportables(Database database) {
		if (generator instanceof ExportableProducer) {
			((ExportableProducer) generator).registerExportables( database );
		}
		if (reactiveGenerator instanceof ExportableProducer) {
			((ExportableProducer) reactiveGenerator).registerExportables( database );
		}
	}
}
