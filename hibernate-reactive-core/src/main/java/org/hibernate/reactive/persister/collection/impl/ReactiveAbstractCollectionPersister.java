/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.ExecuteUpdateResultCheckStyle;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.exception.spi.SQLExceptionConverter;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.jdbc.Expectation;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import org.jboss.logging.Logger;

import static org.hibernate.jdbc.Expectations.appropriateExpectation;
import static org.hibernate.pretty.MessageHelper.collectionInfoString;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Reactive version of {@link org.hibernate.persister.collection.AbstractCollectionPersister}
 */
public interface ReactiveAbstractCollectionPersister extends ReactiveCollectionPersister {
    CoreMessageLogger LOG = Logger.getMessageLogger(
            CoreMessageLogger.class,
            ReactiveBasicCollectionPersister.class.getName()
    );

    default ReactiveConnection getReactiveConnection(SharedSessionContractImplementor session) {
        return ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#recreate(PersistentCollection, Serializable, SharedSessionContractImplementor)
     */
    default CompletionStage<Void> recreateReactive(
            PersistentCollection collection,
            Serializable id,
            SharedSessionContractImplementor session)
            throws HibernateException {
        if ( isInverse() || !isRowInsertEnabled() ) {
            return voidFuture();
        }

        if ( LOG.isDebugEnabled() ) {
            LOG.debugf(
                    "Inserting collection: %s",
                    collectionInfoString( this, collection, id, session )
            );
        }

        ReactiveConnection connection = getReactiveConnection( session );
        //TODO: compose() reactive version of collection.preInsert()
        Iterator<?> entries = collection.entries( this );
        Expectation expectation = appropriateExpectation( getInsertCheckStyle() );
        return loop(
                entries,
                collection::entryExists,
                (entry, index) -> connection.update(
                        getSQLInsertRowString(),
                        insertRowsParamValues( entry, index, collection, id, session ),
                        expectation.canBeBatched(),
                        new ExpectationAdaptor( expectation, getSQLInsertRowString(), getSqlExceptionConverter() )
                )
        );
        //TODO: compose() reactive version of collection.afterRowInsert()
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#remove(Serializable, SharedSessionContractImplementor)
     */
    default CompletionStage<Void> removeReactive(Serializable id, SharedSessionContractImplementor session) {
        if ( isInverse() || !isRowDeleteEnabled() ) {
            return voidFuture();
        }

        if ( LOG.isDebugEnabled() ) {
            LOG.debugf(
                    "Deleting collection: %s",
                    collectionInfoString( this, id, getFactory() )
            );
        }

        Expectation expectation = appropriateExpectation( getDeleteCheckStyle() );
        return getReactiveConnection( session ).update(
                getSQLDeleteString(),
                new Object[]{ id },
                expectation.canBeBatched(),
                new ExpectationAdaptor( expectation, getSQLDeleteString(), getSqlExceptionConverter() )
        );
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#deleteRows(PersistentCollection, Serializable, SharedSessionContractImplementor)
     */
    @Override
    default CompletionStage<Void> reactiveDeleteRows(
            PersistentCollection collection,
            Serializable id,
            SharedSessionContractImplementor session) {
        if ( isInverse() || !isRowDeleteEnabled() ) {
            return voidFuture();
        }

        Iterator<?> deletes = collection.getDeletes( this, !deleteByIndex() );
        if ( !deletes.hasNext() ) {
             return voidFuture();
        }

        ReactiveConnection connection = getReactiveConnection(session);
        Expectation expectation = appropriateExpectation( getDeleteCheckStyle() );
        return loop(
                deletes,
                (entry, index) -> connection.update(
                        getSQLDeleteRowString(),
                        deleteRowsParamValues( entry, 1, id, session ),
                        expectation.canBeBatched(),
                        new ExpectationAdaptor( expectation, getSQLDeleteRowString(), getSqlExceptionConverter() )
                )
        );
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#insertRows(PersistentCollection, Serializable, SharedSessionContractImplementor)
     */
    @Override
    default CompletionStage<Void> reactiveInsertRows(
            PersistentCollection collection,
            Serializable id,
            SharedSessionContractImplementor session) {
        if ( isInverse() || !isRowDeleteEnabled() ) {
            return voidFuture();
        }

        if ( LOG.isDebugEnabled() ) {
            LOG.debugf(
                    "Inserting rows of collection: %s",
                    collectionInfoString( this, collection, id, session )
            );
        }

        ReactiveConnection connection = getReactiveConnection( session );
        //TODO: compose() reactive version of collection.preInsert()
        List<Object> entries = entryList( collection );
        if ( !needsInsert( collection, entries ) ) {
            return voidFuture();
        }

        Expectation expectation = appropriateExpectation( getInsertCheckStyle() );
        return loop(
                entries.iterator(),
                (entry, index) -> collection.needsInserting( entry, index, getElementType() ),
                (entry, index) -> connection.update(
                        getSQLInsertRowString(),
                        insertRowsParamValues( entry, index, collection, id, session ),
                        expectation.canBeBatched(),
                        new ExpectationAdaptor( expectation, getSQLInsertRowString(), getSqlExceptionConverter() ) )
                //TODO: compose() a reactive version of collection.afterRowInsert()
        ).thenAccept( total -> LOG.debugf( "Done inserting rows: %s inserted", total ) );
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#updateRows(PersistentCollection, Serializable, SharedSessionContractImplementor)
     */
    @Override
    default CompletionStage<Void> reactiveUpdateRows(
            PersistentCollection collection,
            Serializable id,
            SharedSessionContractImplementor session) {

        if ( !isInverse() && collection.isRowUpdatePossible() ) {

            if ( LOG.isDebugEnabled() ) {
                LOG.debugf(
                        "Updating rows of collection: %s#%s",
                        collectionInfoString( this, collection, id, session )
                );
            }

            // update all the modified entries
            return doReactiveUpdateRows( id, collection, session );
        }
        return voidFuture();
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#doUpdateRows(Serializable, PersistentCollection, SharedSessionContractImplementor)
     */
    CompletionStage<Void> doReactiveUpdateRows(Serializable id, PersistentCollection collection,
                                                  SharedSessionContractImplementor session);

    default Object[] insertRowsParamValues(Object entry, int index,
                                           PersistentCollection collection, Serializable id,
                                           SharedSessionContractImplementor session) {
        int offset = 1;
        return PreparedStatementAdaptor.bind(
                st -> {
                    int loc = writeKey( st, id, offset , session );
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

    default Object[] deleteRowsParamValues(Object entry, int offset, Serializable id,
                                           SharedSessionContractImplementor session) {
        return PreparedStatementAdaptor.bind(
                st -> {
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
    int writeKey(PreparedStatement st, Serializable id, int offset, SharedSessionContractImplementor session)
            throws SQLException;
    int writeElementToWhere(PreparedStatement st, Object entry, int loc, SharedSessionContractImplementor session)
            throws SQLException;
    int writeIndexToWhere(PreparedStatement st, Object entry, int loc, SharedSessionContractImplementor session)
            throws SQLException;

    String getSQLInsertRowString();
    String getSQLDeleteRowString();
    String getSQLDeleteString();
    String getSQLUpdateRowString();

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
