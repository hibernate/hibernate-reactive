/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import org.hibernate.HibernateException;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.jboss.logging.Logger;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

import static org.hibernate.pretty.MessageHelper.collectionInfoString;
import static org.hibernate.reactive.util.impl.CompletionStages.total;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.zeroFuture;

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

        ReactiveConnection reactiveConnection = getReactiveConnection( session );
        //TODO: compose() reactive version of collection.preInsert()
        Iterator<?> entries = collection.entries( this );
        return total(
                entries,
                (entry, index) -> {
                    if ( collection.entryExists( entry, index ) ) {
                        return reactiveConnection.update(
                                getSQLInsertRowString(),
                                insertRowsParamValues( entry, index, collection, id, session )
                        );
                        //TODO: compose() reactive version of collection.afterRowInsert()
                    }
                    else {
                        return zeroFuture();
                    }
                }
        ).thenAccept( total -> LOG.debugf( "Done inserting rows: %s inserted", total ) );
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#remove(Serializable, SharedSessionContractImplementor)
     */
    default CompletionStage<Void> removeReactive(Serializable id, SharedSessionContractImplementor session) {
        if ( !isInverse() && isRowDeleteEnabled() ) {
            if ( LOG.isDebugEnabled() ) {
                LOG.debugf(
                        "Deleting collection: %s",
                        collectionInfoString( this, id, getFactory() )
                );
            }

            return getReactiveConnection( session )
                    .update( getSQLDeleteString(), new Object[]{ id } )
                    .thenCompose( CompletionStages::voidFuture );
        }
        return voidFuture();
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
        return total(
                deletes,
                (entry, index) -> getReactiveConnection( session ).update(
                        getSQLDeleteRowString(),
                        deleteRowsParamValues( entry, index+1, id, session )
                )
        ).thenAccept( total -> LOG.debugf( "Done removing rows: %s removed", total ) );
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
            return doReactiveUpdateRows( id, collection, session )
                    .thenAccept( (count) -> LOG.debugf( "Done updating rows: %s updated", count ) );
        }
        return voidFuture();
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#doUpdateRows(Serializable, PersistentCollection, SharedSessionContractImplementor)
     */
    CompletionStage<Integer> doReactiveUpdateRows(Serializable id, PersistentCollection collection,
                                          SharedSessionContractImplementor session);

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

        ReactiveConnection reactiveConnection = getReactiveConnection( session );
        //TODO: compose() reactive version of collection.preInsert()
        Iterator<?> entries = collection.entries(this );
        return total(
                entries,
                (entry, index) -> {
                    if ( collection.needsInserting( entry, index, getElementType() ) ) {
                        return reactiveConnection.update(
                                getSQLInsertRowString(),
                                insertRowsParamValues( entry, index, collection, id, session )
                        );
                        //TODO: compose() a reactive version of collection.afterRowInsert()
                    }
                    else {
                        return zeroFuture();
                    }
                }
        ).thenAccept( total -> LOG.debugf( "Done inserting rows: %s inserted", total ) );
    }

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
                    if ( hasIndex() /* && !indexIsFormula */) {
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
}
