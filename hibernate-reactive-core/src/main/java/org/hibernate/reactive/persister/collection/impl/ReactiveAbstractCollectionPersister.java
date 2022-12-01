/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.ExecuteUpdateResultCheckStyle;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.exception.spi.SQLExceptionConverter;
import org.hibernate.jdbc.Expectation;
import org.hibernate.loader.ast.spi.CollectionLoader;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.loader.ast.internal.ReactiveCollectionLoaderBatchKey;
import org.hibernate.reactive.loader.ast.internal.ReactiveCollectionLoaderSingleKey;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;


/**
 * Reactive version of {@link org.hibernate.persister.collection.AbstractCollectionPersister}
 */
public interface ReactiveAbstractCollectionPersister extends ReactiveCollectionPersister {

    Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

    default ReactiveConnection getReactiveConnection(SharedSessionContractImplementor session) {
        return ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
    }

    default CollectionLoader createReactiveCollectionLoader(LoadQueryInfluencers loadQueryInfluencers) {
        final int batchSize = getBatchSize();
        if ( batchSize > 1 ) {
            return new ReactiveCollectionLoaderBatchKey( getAttributeMapping(), batchSize, loadQueryInfluencers, getFactory() );
        }

        return new ReactiveCollectionLoaderSingleKey( getAttributeMapping(), loadQueryInfluencers, getFactory() );
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#recreate(PersistentCollection, Object, SharedSessionContractImplementor)
     */
    default CompletionStage<Void> recreateReactive(
            PersistentCollection collection,
            Object id,
            SharedSessionContractImplementor session)
            throws HibernateException {
        return null;
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#remove(Object, SharedSessionContractImplementor)
     */
    default CompletionStage<Void> removeReactive(Object id, SharedSessionContractImplementor session) {
        return null;
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#deleteRows(PersistentCollection, Object, SharedSessionContractImplementor)
     */
    @Override
    default CompletionStage<Void> reactiveDeleteRows(PersistentCollection collection, Object id, SharedSessionContractImplementor session) {
        return null;
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#insertRows(PersistentCollection, Object, SharedSessionContractImplementor)
     */
    @Override
    default CompletionStage<Void> reactiveInsertRows(PersistentCollection collection, Object id, SharedSessionContractImplementor session) {
        return null;
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#updateRows(PersistentCollection, Object, SharedSessionContractImplementor)
     */
    @Override
    default CompletionStage<Void> reactiveUpdateRows(PersistentCollection collection, Object id, SharedSessionContractImplementor session) {
        return null;
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#doUpdateRows(Object, PersistentCollection, SharedSessionContractImplementor)
     */
    CompletionStage<Void> doReactiveUpdateRows(Object id, PersistentCollection collection, SharedSessionContractImplementor session);

    default Object[] insertRowsParamValues(Object entry, int index, PersistentCollection collection, Object id, SharedSessionContractImplementor session) {
        int offset = 1;
        return PreparedStatementAdaptor
                .bind( st -> {
                           int loc = writeKey( st, id, offset, session );
                           if ( hasIdentifier() ) {
                               loc = writeIdentifier( st, collection.getIdentifier( entry, index ), loc, session );
                           }
                           if ( hasIndex() && !indexContainsFormula() ) {
                               loc = writeIndex( st, collection.getIndex( entry, index, this ), loc, session );
                           }
                           writeElement( st, collection.getElement( entry ), loc, session );
                       }
                );
    }

    default Object[] deleteRowsParamValues(Object entry, int offset, Object id, SharedSessionContractImplementor session) {
        return PreparedStatementAdaptor
                .bind( st -> {
                           int loc = offset;
                           if ( hasIdentifier() ) {
                               writeIdentifier( st, entry, loc, session );
                           }
                           else {
                               loc = writeKey( st, id, loc, session );
                               if ( deleteByIndex() ) {
                                   writeIndexToWhere( st, entry, loc, session );
                               }
                               else {
                                   writeElementToWhere( st, entry, loc, session );
                               }
                           }
                       }
                );
    }

    default boolean deleteByIndex() {
        return !isOneToMany() && hasIndex() && !indexContainsFormula();
    }

    boolean isRowDeleteEnabled();
    boolean isRowInsertEnabled();

    boolean hasIdentifier();
    boolean indexContainsFormula();

    ExecuteUpdateResultCheckStyle getInsertCheckStyle();
    ExecuteUpdateResultCheckStyle getDeleteCheckStyle();

    int writeElement(PreparedStatement st, Object element, int loc, SharedSessionContractImplementor session)
            throws SQLException;
    int writeIndex(PreparedStatement st, Object index, int loc, SharedSessionContractImplementor session)
            throws SQLException;
    int writeIdentifier(PreparedStatement st, Object identifier, int loc, SharedSessionContractImplementor session)
            throws SQLException;
    int writeKey(PreparedStatement st, Object id, int offset, SharedSessionContractImplementor session)
            throws SQLException;
    int writeElementToWhere(PreparedStatement st, Object entry, int loc, SharedSessionContractImplementor session)
            throws SQLException;
    int writeIndexToWhere(PreparedStatement st, Object entry, int loc, SharedSessionContractImplementor session)
            throws SQLException;

    default List<Object> entryList(PersistentCollection collection) {
        Iterator<?> entries = collection.entries( this );
        List<Object> elements = new ArrayList<>();
        while ( entries.hasNext() ) {
            elements.add( entries.next() );
        }
        return elements;
    }

    default boolean needsUpdate(PersistentCollection collection, List<Object> entries) {
        for ( int i = 0, size = entries.size(); i < size; i++ ) {
            Object element = entries.get(i);
            if ( collection.needsUpdating( element, i, getElementType() ) ) {
                return true;
            }
        }
        return false;
    }

    default boolean needsInsert(PersistentCollection collection, List<Object> entries) {
        for ( int i = 0, size = entries.size(); i < size; i++ ) {
            Object element = entries.get(i);
            if ( collection.needsInserting( element, i, getElementType() ) ) {
                return true;
            }
        }
        return false;
    }

    class ExpectationAdaptor implements ReactiveConnection.Expectation {
        private Expectation expectation;
        private String sql;
        private SQLExceptionConverter converter;

        ExpectationAdaptor(Expectation expectation, String sql, SQLExceptionConverter converter) {
            this.expectation = expectation;
            this.sql = sql;
            this.converter = converter;
        }
        @Override
        public void verifyOutcome(int rowCount, int batchPosition, String sql) {
            try {
                expectation.verifyOutcome( rowCount, new PreparedStatementAdaptor(), batchPosition, sql );
            }
            catch ( SQLException sqle ) {
                throw converter.convert( sqle, "could not update collection row", sql );
            }
        }
    }

    default SQLExceptionConverter getSqlExceptionConverter() {
        return getFactory().getJdbcServices().getSqlExceptionHelper().getSqlExceptionConverter();
    }
}
