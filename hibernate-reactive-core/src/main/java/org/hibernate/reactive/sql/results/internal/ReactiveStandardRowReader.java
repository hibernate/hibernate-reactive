/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.EntityKey;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.results.graph.ReactiveDomainResultsAssembler;
import org.hibernate.reactive.sql.results.graph.ReactiveInitializer;
import org.hibernate.reactive.sql.results.spi.ReactiveRowReader;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.graph.InitializerData;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMappingResolution;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;
import org.hibernate.sql.results.spi.RowTransformer;
import org.hibernate.type.descriptor.java.JavaType;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.results.LoadingLogger.LOGGER;


/**
 * @see org.hibernate.sql.results.internal.StandardRowReader
 */
public class ReactiveStandardRowReader<R> implements ReactiveRowReader<R> {

	private static final Log LOG = make( Log.class, lookup() );

	private final DomainResultAssembler<?>[] resultAssemblers;
	private final Initializer<InitializerData>[] resultInitializers;
	private final InitializerData[] resultInitializersData;
	private final Initializer<InitializerData>[] initializers;
	private final InitializerData[] initializersData;
	private final Initializer<InitializerData>[] sortedForResolveInstance;
	private final InitializerData[] sortedForResolveInstanceData;
	private final boolean hasCollectionInitializers;
	private final RowTransformer<R> rowTransformer;
	private final Class<R> domainResultJavaType;

	private final ComponentType componentType;
	private final Class<?> resultElementClass;

	private final int assemblerCount;

	public ReactiveStandardRowReader(
			JdbcValuesMappingResolution jdbcValuesMappingResolution,
			RowTransformer<R> rowTransformer,
			Class<R> domainResultJavaType) {
		this(
				jdbcValuesMappingResolution.getDomainResultAssemblers(),
				jdbcValuesMappingResolution.getResultInitializers(),
				jdbcValuesMappingResolution.getInitializers(),
				jdbcValuesMappingResolution.getSortedForResolveInstance(),
				jdbcValuesMappingResolution.hasCollectionInitializers(),
				rowTransformer,
				domainResultJavaType
		);
	}

	public ReactiveStandardRowReader(
			DomainResultAssembler<?>[] resultAssemblers,
			Initializer<?>[] resultInitializers,
			Initializer<?>[] initializers,
			Initializer<?>[] sortedForResolveInitializers,
			boolean hasCollectionInitializers,
			RowTransformer<R> rowTransformer,
			Class<R> domainResultJavaType) {
		this.resultAssemblers = resultAssemblers;
		this.resultInitializers = (Initializer<InitializerData>[]) resultInitializers;
		this.resultInitializersData = new InitializerData[resultInitializers.length];
		this.initializers = (Initializer<InitializerData>[]) initializers;
		this.initializersData = new InitializerData[initializers.length];
		this.sortedForResolveInstance = (Initializer<InitializerData>[]) sortedForResolveInitializers;
		this.sortedForResolveInstanceData = new InitializerData[sortedForResolveInstance.length];
		this.hasCollectionInitializers = hasCollectionInitializers;
		this.rowTransformer = rowTransformer;
		this.domainResultJavaType = domainResultJavaType;
		this.assemblerCount = resultAssemblers.length;
		if ( domainResultJavaType == null
				|| domainResultJavaType == Object[].class
				|| domainResultJavaType == Object.class
				|| !domainResultJavaType.isArray()
				|| resultAssemblers.length == 1
				&& domainResultJavaType == resultAssemblers[0].getAssembledJavaType().getJavaTypeClass() ) {
			this.resultElementClass = Object.class;
			this.componentType = ComponentType.OBJECT;
		}
		else {
			this.resultElementClass = domainResultJavaType.getComponentType();
			this.componentType = ComponentType.determineComponentType( domainResultJavaType );
		}
	}

	@Override
	public int getInitializerCount() {
		return initializers.length;
	}

	@Override
	public boolean hasCollectionInitializers() {
		return hasCollectionInitializers;
	}

	@Override
	public R readRow(RowProcessingState processingState) {
		throw LOG.nonReactiveMethodCall( "reactiveReadRow" );
	}

	@Override
	public CompletionStage<R> reactiveReadRow(ReactiveRowProcessingState rowProcessingState, JdbcValuesSourceProcessingOptions options) {
		LOGGER.trace( "ReactiveStandardRowReader#readRow" );

		return coordinateInitializers( rowProcessingState )
				.thenCompose( v -> {
					// Copied from Hibernate ORM:
					// "The following is ugly, but unfortunately necessary to not hurt performance.
					// This implementation was micro-benchmarked and discussed with Francesco Nigro,
					// who hinted that using this style instead of the reflective Array.getLength(), Array.set()
					// is easier for the JVM to optimize"
					switch ( componentType ) {
						case BOOLEAN:
							return booleanComponent( resultAssemblers, rowProcessingState, options );
						case BYTE:
							return byteComponent( resultAssemblers, rowProcessingState, options );
						case CHAR:
							return charComponent( resultAssemblers, rowProcessingState, options );
						case SHORT:
							return shortComponent( resultAssemblers, rowProcessingState, options );
						case INT:
							return intComponent( resultAssemblers, rowProcessingState, options );
						case LONG:
							return longComponent( resultAssemblers, rowProcessingState, options );
						case FLOAT:
							return floatComponent( resultAssemblers, rowProcessingState, options );
						case DOUBLE:
							return doubleComponent( resultAssemblers, rowProcessingState, options );
						default:
							return objectComponent( resultAssemblers, rowProcessingState, options );
					}
				} );
	}

	private CompletionStage<R> booleanComponent(
			DomainResultAssembler<?>[] resultAssemblers,
			ReactiveRowProcessingState rowProcessingState,
			JdbcValuesSourceProcessingOptions options) {
		final boolean[] resultRow = new boolean[resultAssemblers.length];
		return loop( 0, assemblerCount, i -> {
			final DomainResultAssembler<?> assembler = resultAssemblers[i];
			LOGGER.debugf( "Calling top-level assembler (%s / %s) : %s", i, assemblerCount, assembler );
			if ( assembler instanceof ReactiveDomainResultsAssembler ) {
				return ( (ReactiveDomainResultsAssembler) assembler )
						.reactiveAssemble( rowProcessingState, options )
						.thenAccept( obj -> resultRow[i] = (boolean) obj );
			}
			resultRow[i] = (boolean) assembler.assemble( rowProcessingState );
			return voidFuture();
		} ).thenApply( ignore -> {
			afterRow( rowProcessingState );
			return (R) resultRow;
		} );
	}

	private CompletionStage<R> byteComponent(
			DomainResultAssembler<?>[] resultAssemblers,
			ReactiveRowProcessingState rowProcessingState,
			JdbcValuesSourceProcessingOptions options) {
		final byte[] resultRow = new byte[resultAssemblers.length];
		return loop( 0, assemblerCount, i -> {
			final DomainResultAssembler<?> assembler = resultAssemblers[i];
			LOGGER.debugf( "Calling top-level assembler (%s / %s) : %s", i, assemblerCount, assembler );
			if ( assembler instanceof ReactiveDomainResultsAssembler ) {
				return ( (ReactiveDomainResultsAssembler) assembler )
						.reactiveAssemble( rowProcessingState, options )
						.thenAccept( obj -> resultRow[i] = (byte) obj );
			}
			resultRow[i] = (byte) assembler.assemble( rowProcessingState );
			return voidFuture();
		} ).thenApply( ignore -> {
			afterRow( rowProcessingState );
			return (R) resultRow;
		} );
	}

	private CompletionStage<R> charComponent(
			DomainResultAssembler<?>[] resultAssemblers,
			ReactiveRowProcessingState rowProcessingState,
			JdbcValuesSourceProcessingOptions options) {
		final char[] resultRow = new char[resultAssemblers.length];
		return loop( 0, assemblerCount, i -> {
			final DomainResultAssembler<?> assembler = resultAssemblers[i];
			LOGGER.debugf( "Calling top-level assembler (%s / %s) : %s", i, assemblerCount, assembler );
			if ( assembler instanceof ReactiveDomainResultsAssembler ) {
				return ( (ReactiveDomainResultsAssembler) assembler )
						.reactiveAssemble( rowProcessingState, options )
						.thenAccept( obj -> resultRow[i] = (char) obj );
			}
			resultRow[i] = (char) assembler.assemble( rowProcessingState );
			return voidFuture();
		} ).thenApply( ignore -> {
			afterRow( rowProcessingState );
			return (R) resultRow;
		} );
	}

	private CompletionStage<R> shortComponent(
			DomainResultAssembler<?>[] resultAssemblers,
			ReactiveRowProcessingState rowProcessingState,
			JdbcValuesSourceProcessingOptions options) {
		final short[] resultRow = new short[resultAssemblers.length];
		return loop( 0, assemblerCount, i -> {
			final DomainResultAssembler<?> assembler = resultAssemblers[i];
			LOGGER.debugf( "Calling top-level assembler (%s / %s) : %s", i, assemblerCount, assembler );
			if ( assembler instanceof ReactiveDomainResultsAssembler ) {
				return ( (ReactiveDomainResultsAssembler) assembler )
						.reactiveAssemble( rowProcessingState, options )
						.thenAccept( obj -> resultRow[i] = (short) obj );
			}
			resultRow[i] = (short) assembler.assemble( rowProcessingState );
			return voidFuture();
		} ).thenApply( ignore -> {
			afterRow( rowProcessingState );
			return (R) resultRow;
		} );
	}

	private CompletionStage<R> intComponent(
			DomainResultAssembler<?>[] resultAssemblers,
			ReactiveRowProcessingState rowProcessingState,
			JdbcValuesSourceProcessingOptions options) {
		final int[] resultRow = new int[resultAssemblers.length];
		return loop( 0, assemblerCount, i -> {
			final DomainResultAssembler<?> assembler = resultAssemblers[i];
			LOGGER.debugf( "Calling top-level assembler (%s / %s) : %s", i, assemblerCount, assembler );
			if ( assembler instanceof ReactiveDomainResultsAssembler ) {
				return ( (ReactiveDomainResultsAssembler) assembler )
						.reactiveAssemble( rowProcessingState, options )
						.thenAccept( obj -> resultRow[i] = (int) obj );
			}
			resultRow[i] = (int) assembler.assemble( rowProcessingState );
			return voidFuture();
		} ).thenApply( ignore -> {
			afterRow( rowProcessingState );
			return (R) resultRow;
		} );
	}

	private CompletionStage<R> longComponent(
			DomainResultAssembler<?>[] resultAssemblers,
			ReactiveRowProcessingState rowProcessingState,
			JdbcValuesSourceProcessingOptions options) {
		final long[] resultRow = new long[resultAssemblers.length];
		return loop( 0, assemblerCount, i -> {
			final DomainResultAssembler<?> assembler = resultAssemblers[i];
			LOGGER.debugf( "Calling top-level assembler (%s / %s) : %s", i, assemblerCount, assembler );
			if ( assembler instanceof ReactiveDomainResultsAssembler ) {
				return ( (ReactiveDomainResultsAssembler) assembler )
						.reactiveAssemble( rowProcessingState, options )
						.thenAccept( obj -> resultRow[i] = (long) obj );
			}
			resultRow[i] = (long) assembler.assemble( rowProcessingState );
			return voidFuture();
		} ).thenApply( ignore -> {
			afterRow( rowProcessingState );
			return (R) resultRow;
		} );
	}

	private CompletionStage<R> floatComponent(
			DomainResultAssembler<?>[] resultAssemblers,
			ReactiveRowProcessingState rowProcessingState,
			JdbcValuesSourceProcessingOptions options) {
		final float[] resultRow = new float[resultAssemblers.length];
		return loop( 0, assemblerCount, i -> {
			final DomainResultAssembler<?> assembler = resultAssemblers[i];
			LOGGER.debugf( "Calling top-level assembler (%s / %s) : %s", i, assemblerCount, assembler );
			if ( assembler instanceof ReactiveDomainResultsAssembler ) {
				return ( (ReactiveDomainResultsAssembler) assembler )
						.reactiveAssemble( rowProcessingState, options )
						.thenAccept( obj -> resultRow[i] = (float) obj );
			}
			resultRow[i] = (float) assembler.assemble( rowProcessingState );
			return voidFuture();
		} ).thenApply( ignore -> {
			afterRow( rowProcessingState );
			return (R) resultRow;
		} );
	}

	private CompletionStage<R> doubleComponent(
			DomainResultAssembler<?>[] resultAssemblers,
			ReactiveRowProcessingState rowProcessingState,
			JdbcValuesSourceProcessingOptions options) {
		final double[] resultRow = new double[resultAssemblers.length];
		return loop( 0, assemblerCount, i -> {
			final DomainResultAssembler<?> assembler = resultAssemblers[i];
			LOGGER.debugf( "Calling top-level assembler (%s / %s) : %s", i, assemblerCount, assembler );
			if ( assembler instanceof ReactiveDomainResultsAssembler ) {
				return ( (ReactiveDomainResultsAssembler) assembler )
						.reactiveAssemble( rowProcessingState, options )
						.thenAccept( obj -> resultRow[i] = (double) obj );
			}
			resultRow[i] = (double) assembler.assemble( rowProcessingState );
			return voidFuture();
		} ).thenApply( ignore -> {
			afterRow( rowProcessingState );
			return (R) resultRow;
		} );
	}

	private CompletionStage<R> objectComponent(
			DomainResultAssembler<?>[] resultAssemblers,
			ReactiveRowProcessingState rowProcessingState,
			JdbcValuesSourceProcessingOptions options) {
		final Object[] resultRow = (Object[]) Array.newInstance( resultElementClass, resultAssemblers.length );
		return loop( 0, assemblerCount, i -> {
			final DomainResultAssembler<?> assembler = resultAssemblers[i];
			LOGGER.debugf( "Calling top-level assembler (%s / %s) : %s", i, assemblerCount, assembler );
			if ( assembler instanceof ReactiveDomainResultsAssembler ) {
				return ( (ReactiveDomainResultsAssembler) assembler )
						.reactiveAssemble( rowProcessingState, options )
						.thenAccept( obj -> resultRow[i] = obj );
			}
			resultRow[i] = assembler.assemble( rowProcessingState );
			return voidFuture();
		} ).thenApply( ignore -> {
			afterRow( rowProcessingState );
			return rowTransformer.transformRow( resultRow );
		} );
	}

	@Override
	public EntityKey resolveSingleResultEntityKey(RowProcessingState rowProcessingState) {
		return null;
	}

	@Override
	public Class<R> getDomainResultResultJavaType() {
		return domainResultJavaType;
	}

	@Override
	public List<JavaType<?>> getResultJavaTypes() {
		List<JavaType<?>> javaTypes = new ArrayList<>( resultAssemblers.length );
		for ( DomainResultAssembler<?> resultAssembler : resultAssemblers ) {
			javaTypes.add( resultAssembler.getAssembledJavaType() );
		}
		return javaTypes;
	}

	private void afterRow(RowProcessingState rowProcessingState) {
		LOGGER.trace( "ReactiveStandardRowReader#afterRow" );
		finishUpRow();
	}

	private void finishUpRow() {
		for ( InitializerData data : initializersData ) {
			data.setState( Initializer.State.UNINITIALIZED );
		}
	}

	private CompletionStage<Void> coordinateInitializers(RowProcessingState rowProcessingState) {
		return loop( 0, resultInitializers.length, i -> resolveKey( resultInitializers[i], resultInitializersData[i] ) )
				.thenCompose( v -> loop( 0, sortedForResolveInstance.length, i -> resolveInstance( sortedForResolveInstance[i], sortedForResolveInstanceData[i] ) ) )
				.thenCompose( v -> loop( 0, initializers.length, i -> initializeInstance( initializers[i], initializersData[i] ) ) );
	}

	private CompletionStage<Void> resolveKey(Initializer<InitializerData> initializer, InitializerData initializerData) {
		if ( initializer instanceof ReactiveInitializer ) {
			return ( (ReactiveInitializer) initializer ).reactiveResolveKey( initializerData );
		}
		initializer.resolveKey( initializerData );
		return voidFuture();
	}

	private CompletionStage<Void> resolveInstance(Initializer<InitializerData> initializer, InitializerData initializerData) {
		if ( initializerData.getState() == Initializer.State.KEY_RESOLVED ) {
			if ( initializer instanceof ReactiveInitializer ) {
				return ( (ReactiveInitializer) initializer ).reactiveResolveInstance( initializerData );
			}
			initializer.resolveInstance( initializerData );
		}
		return voidFuture();
	}

	private CompletionStage<Void> initializeInstance(Initializer<InitializerData> initializer, InitializerData initializerData) {
		if ( initializerData.getState() == Initializer.State.RESOLVED ) {
			if ( initializer instanceof ReactiveInitializer ) {
				return ( (ReactiveInitializer) initializer ).reactiveInitializeInstance( initializerData );
			}
			initializer.initializeInstance( initializerData );
		}
		return voidFuture();
	}

	@Override
	public void startLoading(RowProcessingState processingState) {
		for ( int i = 0; i < resultInitializers.length; i++ ) {
			final Initializer<?> initializer = resultInitializers[i];
			initializer.startLoading( processingState );
			resultInitializersData[i] = initializer.getData( processingState );
		}
		for ( int i = 0; i < sortedForResolveInstance.length; i++ ) {
			sortedForResolveInstanceData[i] = sortedForResolveInstance[i].getData( processingState );
		}
		for ( int i = 0; i < initializers.length; i++ ) {
			initializersData[i] = initializers[i].getData( processingState );
		}
	}

	@Override
	public void finishUp(RowProcessingState rowProcessingState) {
		for ( int i = 0; i < initializers.length; i++ ) {
			initializers[i].endLoading( initializersData[i] );
		}
	}

	enum ComponentType {
		BOOLEAN( boolean.class ),
		BYTE( byte.class ),
		SHORT( short.class ),
		CHAR( char.class ),
		INT( int.class ),
		LONG( long.class ),
		FLOAT( float.class ),
		DOUBLE( double.class ),
		OBJECT( Object.class );

		private final Class<?> componentType;

		ComponentType(Class<?> componentType) {
			this.componentType = componentType;
		}

		public static ComponentType determineComponentType(Class<?> resultType) {
			if ( resultType == boolean[].class ) {
				return BOOLEAN;
			}
			if ( resultType == byte[].class ) {
				return BYTE;
			}
			if ( resultType == short[].class ) {
				return SHORT;
			}
			if ( resultType == char[].class ) {
				return CHAR;
			}
			if ( resultType == int[].class ) {
				return INT;
			}
			if ( resultType == long[].class ) {
				return LONG;
			}
			if ( resultType == float[].class ) {
				return FLOAT;
			}
			if ( resultType == double[].class ) {
				return DOUBLE;
			}
			return OBJECT;
		}

		public Class<?> getComponentType() {
			return componentType;
		}
	}
}
