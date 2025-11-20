/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal;

import org.hibernate.LockOptions;
import org.hibernate.dialect.lock.spi.LockTimeoutType;
import org.hibernate.dialect.lock.spi.LockingSupport;
import org.hibernate.internal.util.collections.CollectionHelper;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.sql.exec.internal.lock.ReactiveConnectionLockTimeoutStrategyBuilder;
import org.hibernate.reactive.sql.exec.internal.lock.ReactiveFollowOnLockingAction;
import org.hibernate.reactive.sql.exec.internal.lock.ReactiveLockTimeoutHandler;
import org.hibernate.reactive.sql.exec.spi.ReactiveJdbcSelect;
import org.hibernate.reactive.sql.exec.spi.ReactivePostAction;
import org.hibernate.reactive.sql.exec.spi.ReactivePreAction;
import org.hibernate.sql.ast.spi.LockingClauseStrategy;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.exec.internal.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.internal.JdbcSelectWithActions;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcSelect;
import org.hibernate.sql.exec.spi.JdbcSelectWithActionsBuilder;
import org.hibernate.sql.exec.spi.LoadedValuesCollector;
import org.hibernate.sql.exec.spi.PostAction;
import org.hibernate.sql.exec.spi.PreAction;
import org.hibernate.sql.exec.spi.SecondaryAction;
import org.hibernate.sql.exec.spi.StatementAccess;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;

/**
 * Reactive version of {@link JdbcSelectWithActions}
 */
public class ReactiveJdbcSelectWithActions extends JdbcSelectWithActions implements ReactiveJdbcSelect {
	public static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	public void performPreActions(
			StatementAccess jdbcStatementAccess,
			Connection jdbcConnection,
			ExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "reactivePerformPreActions()" );

	}

	@Override
	public void performPostAction(
			boolean succeeded,
			StatementAccess jdbcStatementAccess,
			Connection jdbcConnection,
			ExecutionContext executionContext) {
		throw LOG.nonReactiveMethodCall( "reactivePerformPostActions()" );
	}

	public ReactiveJdbcSelectWithActions(
			JdbcOperationQuerySelect primaryOperation,
			LoadedValuesCollector loadedValuesCollector,
			PreAction[] preActions,
			PostAction[] postActions) {
		super( primaryOperation, loadedValuesCollector, preActions, postActions );
	}

	public ReactiveJdbcSelectWithActions(
			JdbcOperationQuerySelect primaryAction,
			LoadedValuesCollector loadedValuesCollector) {
		super( primaryAction, loadedValuesCollector );
	}

	@Override
	public CompletionStage<Void> reactivePerformPreActions(
			ReactiveConnection connection,
			ExecutionContext executionContext) {
		if ( preActions == null ) {
			return nullFuture();
		}

		return loop( preActions, preAction ->
						( (ReactivePreAction) preAction ).reactivePerformPreAction( connection, executionContext )
		);
	}

	@Override
	public CompletionStage<Void> reactivePerformPostActions(
			boolean succeeded,
			ReactiveConnection connection,
			ExecutionContext executionContext) {
		if ( postActions != null ) {
			return loop(
					postActions, postAction -> {
						if ( succeeded || postAction.shouldRunAfterFail() ) {
							return ( (ReactivePostAction) postAction ).reactivePerformReactivePostAction(
									connection,
									executionContext
							);
						}
						return nullFuture();
					}
			).thenAccept( unused -> {
				if ( loadedValuesCollector != null ) {
					loadedValuesCollector.clear();
				}
			} );
		}
		else {
			if ( loadedValuesCollector != null ) {
				loadedValuesCollector.clear();
			}
			return nullFuture();
		}
	}

	public static class Builder implements JdbcSelectWithActionsBuilder {
		private JdbcOperationQuerySelect primaryAction;
		private LoadedValuesCollector loadedValuesCollector;
		protected List<PreAction> preActions;
		protected List<PostAction> postActions;
		protected LockTimeoutType lockTimeoutType;
		protected LockingSupport lockingSupport;
		protected LockOptions lockOptions;
		protected QuerySpec lockingTarget;
		protected LockingClauseStrategy lockingClauseStrategy;
		boolean isFollowOnLockStrategy;

		@Override
		public Builder setPrimaryAction(JdbcSelect primaryAction) {
			assert primaryAction instanceof JdbcOperationQuerySelect;
			this.primaryAction = (JdbcOperationQuerySelect) primaryAction;
			return this;
		}

		@SuppressWarnings("UnusedReturnValue")
		@Override
		public Builder setLoadedValuesCollector(LoadedValuesCollector loadedValuesCollector) {
			this.loadedValuesCollector = loadedValuesCollector;
			return this;
		}

		@Override
		public Builder setLockTimeoutType(LockTimeoutType lockTimeoutType) {
			this.lockTimeoutType = lockTimeoutType;
			return this;
		}

		@Override
		public Builder setLockingSupport(LockingSupport lockingSupport) {
			this.lockingSupport = lockingSupport;
			return this;
		}

		@Override
		public Builder setLockOptions(LockOptions lockOptions) {
			this.lockOptions = lockOptions;
			return this;
		}

		@Override
		public Builder setLockingTarget(QuerySpec lockingTarget) {
			this.lockingTarget = lockingTarget;
			return this;
		}

		@Override
		public Builder setLockingClauseStrategy(LockingClauseStrategy lockingClauseStrategy) {
			this.lockingClauseStrategy = lockingClauseStrategy;
			return this;
		}

		@Override
		public Builder setIsFollowOnLockStrategy(boolean isFollowOnLockStrategy) {
			this.isFollowOnLockStrategy = isFollowOnLockStrategy;
			return this;
		}

		@Override
		public JdbcSelect build() {
			if ( lockTimeoutType == LockTimeoutType.CONNECTION ) {
				addSecondaryActionPair(
						new ReactiveLockTimeoutHandler(
								lockOptions.getTimeout(),
								ReactiveConnectionLockTimeoutStrategyBuilder.build( lockingSupport.getConnectionLockTimeoutStrategy() )
						)
				);
			}
			if ( isFollowOnLockStrategy ) {
				ReactiveFollowOnLockingAction.apply( lockOptions, lockingTarget, lockingClauseStrategy, this );
			}

			if ( preActions == null && postActions == null ) {
				assert loadedValuesCollector == null;
				return primaryAction;
			}
			final PreAction[] preActions = toPreActionArray( this.preActions );
			final PostAction[] postActions = toPostActionArray( this.postActions );
			return new ReactiveJdbcSelectWithActions( primaryAction, loadedValuesCollector, preActions, postActions );
		}

		/**
		 * Appends the {@code actions} to the growing list of pre-actions,
		 * executed (in order) after all currently registered actions.
		 *
		 * @return {@code this}, for method chaining.
		 */
		@Override
		public Builder appendPreAction(PreAction... actions) {
			if ( preActions == null ) {
				preActions = new ArrayList<>();
			}
			Collections.addAll( preActions, actions );
			return this;
		}

		/**
		 * Prepends the {@code actions} to the growing list of pre-actions
		 *
		 * @return {@code this}, for method chaining.
		 */
		@Override
		public Builder prependPreAction(PreAction... actions) {
			if ( preActions == null ) {
				preActions = new ArrayList<>();
			}
			// todo (DatabaseOperation) : should we invert the order of the incoming actions?
			Collections.addAll( preActions, actions );
			return this;
		}

		/**
		 * Appends the {@code actions} to the growing list of post-actions
		 *
		 * @return {@code this}, for method chaining.
		 */
		@Override
		public Builder appendPostAction(PostAction... actions) {
			if ( postActions == null ) {
				postActions = new ArrayList<>();
			}
			Collections.addAll( postActions, actions );
			return this;
		}

		/**
		 * Prepends the {@code actions} to the growing list of post-actions
		 *
		 * @return {@code this}, for method chaining.
		 */
		@Override
		public Builder prependPostAction(PostAction... actions) {
			if ( postActions == null ) {
				postActions = new ArrayList<>();
			}
			// todo (DatabaseOperation) : should we invert the order of the incoming actions?
			Collections.addAll( postActions, actions );
			return this;
		}

		/**
		 * Adds a secondary action pair.
		 * Assumes the {@code action} implements both {@linkplain PreAction} and {@linkplain PostAction}.
		 *
		 * @return {@code this}, for method chaining.
		 *
		 * @apiNote Prefer {@linkplain #addSecondaryActionPair(PreAction, PostAction)} to avoid
		 * the casts needed here.
		 * @see #prependPreAction
		 * @see #appendPostAction
		 */
		@Override
		public Builder addSecondaryActionPair(SecondaryAction action) {
			return addSecondaryActionPair( (PreAction) action, (PostAction) action );
		}

		/**
		 * Adds a PreAction/PostAction pair.
		 *
		 * @return {@code this}, for method chaining.
		 *
		 * @see #prependPreAction
		 * @see #appendPostAction
		 */
		@Override
		public Builder addSecondaryActionPair(PreAction preAction, PostAction postAction) {
			prependPreAction( preAction );
			appendPostAction( postAction );
			return this;
		}

		static PreAction[] toPreActionArray(List<PreAction> actions) {
			if ( CollectionHelper.isEmpty( actions ) ) {
				return null;
			}
			return actions.toArray( new PreAction[0] );
		}

		static PostAction[] toPostActionArray(List<PostAction> actions) {
			if ( CollectionHelper.isEmpty( actions ) ) {
				return null;
			}
			return actions.toArray( new PostAction[0] );
		}

	}
}
