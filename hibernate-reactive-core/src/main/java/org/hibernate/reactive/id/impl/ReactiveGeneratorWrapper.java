/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.ExportableProducer;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import static java.util.function.Function.identity;

/**
 * @author Gavin King
 */
public class ReactiveGeneratorWrapper
		implements IdentifierGenerator, ExportableProducer, ReactiveIdentifierGenerator<Object> {

	private final ReactiveIdentifierGenerator<?> reactiveGenerator;
	private final IdentifierGenerator generator;

	public ReactiveGeneratorWrapper(ReactiveIdentifierGenerator<?> reactiveGenerator) {
		this( reactiveGenerator, null );
	}

	public ReactiveGeneratorWrapper(ReactiveIdentifierGenerator<?> reactiveGenerator, IdentifierGenerator generator) {
		this.reactiveGenerator = reactiveGenerator;
		this.generator = generator;
	}

	@Override
	public void initialize(SqlStringGenerationContext context) {
		if ( reactiveGenerator != null ) {
			reactiveGenerator.initialize( context );
		}
		if ( generator != null ) {
			generator.initialize( context );
		}
	}

	@Override
	public CompletionStage<Object> generate(ReactiveConnectionSupplier session, Object entity) {
		return reactiveGenerator.generate( session, entity ).thenApply( identity() );
	}

	@Override
	public Object generate(SharedSessionContractImplementor session, Object object) {
		Objects.requireNonNull( generator, "Only a reactive generator is available" );
		return generator.generate( session, object );
	}

	@Override
	public void registerExportables(Database database) {
		if ( generator != null ) {
			generator.registerExportables( database );
		}
		if ( reactiveGenerator != null ) {
			reactiveGenerator.registerExportables( database );
		}
	}
}
