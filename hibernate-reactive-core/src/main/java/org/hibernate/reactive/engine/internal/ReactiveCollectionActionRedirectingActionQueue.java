/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.internal;

import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Set;

import org.hibernate.HibernateException;
import org.hibernate.PropertyValueException;
import org.hibernate.Session;
import org.hibernate.action.internal.BulkOperationCleanupAction;
import org.hibernate.action.internal.EntityDeleteAction;
import org.hibernate.action.internal.EntityIdentityInsertAction;
import org.hibernate.action.internal.EntityInsertAction;
import org.hibernate.action.internal.EntityUpdateAction;
import org.hibernate.action.internal.OrphanRemovalAction;
import org.hibernate.action.queue.spi.ActionQueue;
import org.hibernate.action.queue.spi.ActionQueueCheckpoint;
import org.hibernate.action.queue.spi.CollectionMutationInput;
import org.hibernate.action.queue.spi.CollectionTransition;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.TransactionCompletionCallbacksImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.session.ReactiveSession;

/**
 * An {@link ActionQueue} wrapper that intercepts collection action scheduling
 * and redirects them to the {@link ReactiveActionQueue}.
 * <p>
 * In ORM 8, {@link org.hibernate.engine.internal.FlushProcessingContext} schedules
 * collection actions (recreate/remove/update) on the session's ActionQueue. Since
 * Hibernate Reactive uses a separate {@link ReactiveActionQueue} for async execution,
 * this wrapper converts non-reactive collection actions into their reactive equivalents
 * and adds them to the ReactiveActionQueue instead.
 */
public class ReactiveCollectionActionRedirectingActionQueue implements ActionQueue {

	private final ActionQueue delegate;
	private final ReactiveSession reactiveSession;
	private final EventSource session;

	public ReactiveCollectionActionRedirectingActionQueue(ActionQueue delegate, ReactiveSession reactiveSession, EventSource session) {
		this.delegate = delegate;
		this.reactiveSession = reactiveSession;
		this.session = session;
	}

	private ReactiveActionQueue reactiveActionQueue() {
		return reactiveSession.getReactiveActionQueue();
	}

	@Override
	public void addCollectionMutation(CollectionMutationInput input) {
		final CollectionTransition transition = input.transition();
		switch ( transition ) {
			case CREATE:
				reactiveActionQueue().addAction(
						new ReactiveCollectionRecreateAction(
								input.collection(),
								input.currentEndpoint().persister(),
								input.currentEndpoint().key(),
								session
						)
				);
				break;
			case REMOVE:
				if ( input.collection() != null ) {
					reactiveActionQueue().addAction(
							new ReactiveCollectionRemoveAction(
									input.collection(),
									input.loadedEndpoint().persister(),
									input.loadedEndpoint().key(),
									input.emptySnapshot(),
									session
							)
					);
				}
				else {
					reactiveActionQueue().addAction(
							new ReactiveCollectionRemoveAction(
									input.loadedEndpoint().persister(),
									input.loadedEndpoint().key(),
									session
							)
					);
				}
				break;
			case UPDATE:
				reactiveActionQueue().addAction(
						new ReactiveCollectionUpdateAction(
								input.collection(),
								input.currentEndpoint().persister(),
								input.currentEndpoint().key(),
								input.emptySnapshot(),
								session
						)
				);
				break;
			case REMOVE_AND_CREATE:
				if ( input.collection() != null ) {
					reactiveActionQueue().addAction(
							new ReactiveCollectionRemoveAction(
									input.collection(),
									input.loadedEndpoint().persister(),
									input.loadedEndpoint().key(),
									input.emptySnapshot(),
									session
							)
					);
				}
				else {
					reactiveActionQueue().addAction(
							new ReactiveCollectionRemoveAction(
									input.loadedEndpoint().persister(),
									input.loadedEndpoint().key(),
									session
							)
					);
				}
				reactiveActionQueue().addAction(
						new ReactiveCollectionRecreateAction(
								input.collection(),
								input.currentEndpoint().persister(),
								input.currentEndpoint().key(),
								session
						)
				);
				break;
			case NONE:
				break;
		}
	}

	// --- Delegate all other methods ---

	@Override
	public void clear() {
		delegate.clear();
	}

	@Override
	public void addAction(EntityInsertAction action) {
		delegate.addAction( action );
	}

	@Override
	public void addAction(EntityIdentityInsertAction action) {
		delegate.addAction( action );
	}

	@Override
	public void addAction(EntityUpdateAction action) {
		delegate.addAction( action );
	}

	@Override
	public void addAction(EntityDeleteAction action) {
		delegate.addAction( action );
	}

	@Override
	public void addAction(OrphanRemovalAction action) {
		delegate.addAction( action );
	}

	@Override
	public void addAction(BulkOperationCleanupAction action) {
		delegate.addAction( action );
	}

	@Override
	public void executeInserts() throws HibernateException {
		delegate.executeInserts();
	}

	@Override
	public void executeActions() throws HibernateException {
		delegate.executeActions();
	}

	@Override
	public void prepareActions() throws HibernateException {
		delegate.prepareActions();
	}

	@Override
	public void executePendingBulkOperationCleanUpActions() {
		delegate.executePendingBulkOperationCleanUpActions();
	}

	@Override
	public boolean hasUnresolvedEntityInsertActions() {
		return delegate.hasUnresolvedEntityInsertActions();
	}

	@Override
	public boolean hasAnyQueuedActions() {
		return delegate.hasAnyQueuedActions();
	}

	@Override
	public boolean hasBeforeTransactionActions() {
		return delegate.hasBeforeTransactionActions();
	}

	@Override
	public boolean hasAfterTransactionActions() {
		return delegate.hasAfterTransactionActions();
	}

	@Override
	public boolean areInsertionsOrDeletionsQueued() {
		return delegate.areInsertionsOrDeletionsQueued();
	}

	@Override
	public boolean areTablesToBeUpdated(Set<? extends Serializable> querySpaces) {
		return delegate.areTablesToBeUpdated( querySpaces );
	}

	@Override
	public void checkNoUnresolvedActionsAfterOperation() throws PropertyValueException {
		delegate.checkNoUnresolvedActionsAfterOperation();
	}

	@Override
	public int numberOfInsertions() {
		return delegate.numberOfInsertions();
	}

	@Override
	public int numberOfUpdates() {
		return delegate.numberOfUpdates();
	}

	@Override
	public int numberOfDeletions() {
		return delegate.numberOfDeletions();
	}

	@Override
	public int numberOfCollectionCreations() {
		return delegate.numberOfCollectionCreations();
	}

	@Override
	public int numberOfCollectionUpdates() {
		return delegate.numberOfCollectionUpdates();
	}

	@Override
	public int numberOfCollectionRemovals() {
		return delegate.numberOfCollectionRemovals();
	}

	@Override
	public TransactionCompletionCallbacksImplementor getTransactionCompletionCallbacks() {
		return delegate.getTransactionCompletionCallbacks();
	}

	@Override
	public void setTransactionCompletionCallbacks(TransactionCompletionCallbacksImplementor callbacks, boolean isTransactionCoordinatorShared) {
		delegate.setTransactionCompletionCallbacks( callbacks, isTransactionCoordinatorShared );
	}

	@Override
	public void beforeTransactionCompletion() {
		delegate.beforeTransactionCompletion();
	}

	@Override
	public void setAuditChangesetContext(Object context, Session session) {
		delegate.setAuditChangesetContext( context, session );
	}

	@Override
	public void afterTransactionCompletion(boolean success) {
		delegate.afterTransactionCompletion( success );
	}

	@Override
	public void sortActions() {
		delegate.sortActions();
	}

	@Override
	public void sortCollectionActions() {
		delegate.sortCollectionActions();
	}

	@Override
	public void unScheduleUnloadedDeletion(Object entity) {
		delegate.unScheduleUnloadedDeletion( entity );
	}

	@Override
	public void unScheduleDeletion(EntityEntry entry, Object rescuedEntity) {
		delegate.unScheduleDeletion( entry, rescuedEntity );
	}

	@Override
	public ActionQueueCheckpoint checkpoint() {
		return delegate.checkpoint();
	}

	@Override
	public void restore(ActionQueueCheckpoint checkpoint) {
		delegate.restore( checkpoint );
	}

	@Override
	public void serialize(ObjectOutputStream oos) throws java.io.IOException {
		delegate.serialize( oos );
	}

	@Override
	public void registerCallback(BeforeCompletionCallback callback) {
		delegate.registerCallback( callback );
	}

	@Override
	public void registerCallback(AfterCompletionCallback callback) {
		delegate.registerCallback( callback );
	}
}
