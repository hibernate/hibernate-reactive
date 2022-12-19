/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.ExportableProducer;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.enhanced.DatabaseStructure;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * @author Gavin King
 */
public class ReactiveGeneratorWrapper<T>
		implements IdentifierGenerator, ExportableProducer, ReactiveIdentifierGenerator<T> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final DatabaseStructure databaseStructure;
	private ReactiveIdentifierGenerator<T> reactiveGenerator;
	private IdentifierGenerator generator;
	private Class<T> returnedClass;

	public ReactiveGeneratorWrapper(ReactiveIdentifierGenerator<T> reactiveGenerator, Class<T> returnedClass) {
		this( reactiveGenerator, null, returnedClass );
	}

	public ReactiveGeneratorWrapper(ReactiveIdentifierGenerator<T> reactiveGenerator, IdentifierGenerator generator, Class<T> returnedClass) {
		this.reactiveGenerator = reactiveGenerator;
		this.generator = generator;
		this.returnedClass = returnedClass;
		this.databaseStructure = generator instanceof SequenceStyleGenerator
				? ( (SequenceStyleGenerator) generator ).getDatabaseStructure()
				: null;
	}

	@Override
	public void initialize(SqlStringGenerationContext context) {
		if ( reactiveGenerator instanceof IdentifierGenerator ) {
			( (IdentifierGenerator) reactiveGenerator ).initialize( context );
		}
		generator.initialize( context );
	}

	@Override
	public CompletionStage<T> generate(ReactiveConnectionSupplier session, Object entity) {
		return reactiveGenerator
				.generate( session, entity )
				.thenApply( id -> {
					//FIXME: this is just a temp workaround
					// The correct approach would be to use an IntegralDataTypeHolder
					if ( Integer.class.equals( returnedClass ) ) {
						return ( (Number) id ).intValue();
					}
					if ( Short.class.equals( returnedClass ) ) {
						return ( (Number) id ).shortValue();
					}
					return id;
				} )
				.thenApply( this::castId );
	}

	private T castId(Object id) {
		return (T) id;
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
		if ( reactiveGenerator instanceof ExportableProducer ) {
			( (ExportableProducer) reactiveGenerator ).registerExportables( database );
		}
	}
}
