/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/quarkusio/quarkus/issues/41198
 * and https://github.com/hibernate/hibernate-reactive/issues/1942
 * <p>
 * Tests that JOIN FETCH queries don't cause orphaned PersistentCollections
 * when using cascade=ALL with orphanRemoval=true.
 * <p>
 * The original bug occurred when:
 * 1. An entity was loaded into the persistence context
 * 2. The same entity was queried again with JOIN FETCH
 * 3. Hibernate created a new PersistentCollection but left the old one in the context
 * 4. Collection modifications triggered orphan removal checks on the old collection
 * 5. HibernateException was thrown incorrectly
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class OrphanRemovalCascadeTest extends BaseReactiveTest {

	@Override
	public CompletionStage<Void> deleteEntities(Class<?>... entities) {
		return getSessionFactory()
			.withTransaction( s -> s
				.createSelectionQuery( "from Post", Post.class )
				.getResultList()
				.thenApply( List::toArray )
				.thenCompose( s::remove )
			);
	}

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Post.class, PostComment.class );
	}

	@Test
	public void testAddCommentToPost(VertxTestContext context) {
		Post post = new Post( "Test Post" );
		PostComment comment = new PostComment( "First Comment" );
		post.addComment( comment );

		test(
			context,
			getSessionFactory()
				.withTransaction( session -> session.persist( post ) )
				.thenCompose( v -> getSessionFactory().withTransaction( session -> session
					.find( Post.class, post.id )
					.thenCompose( foundPost -> session
						.fetch( foundPost.comments )
						.thenAccept( comments -> assertThat( comments )
							.hasSize( 1 )
							.extracting( PostComment::getContent )
							.containsExactly( "First Comment" )
						)
					)
				) )
		);
	}

	@Test
	public void testAddMultipleCommentsToPost(VertxTestContext context) {
		Post post = new Post( "Test Post" );
		post.addComment( new PostComment( "Comment 1" ) );
		post.addComment( new PostComment( "Comment 2" ) );
		post.addComment( new PostComment( "Comment 3" ) );

		test(
			context,
			getSessionFactory()
				.withTransaction( session -> session.persist( post ) )
				.thenCompose( v -> getSessionFactory().withTransaction( session -> session
					.find( Post.class, post.id )
					.thenCompose( foundPost -> session
						.fetch( foundPost.comments )
						.thenAccept( comments -> assertThat( comments )
							.hasSize( 3 )
							.extracting( PostComment::getContent )
							.containsExactlyInAnyOrder( "Comment 1", "Comment 2", "Comment 3" )
						)
					)
				) )
		);
	}

	@Test
	public void testRemoveCommentFromPost(VertxTestContext context) {
		Post post = new Post( "Test Post" );
		PostComment comment1 = new PostComment( "Comment 1" );
		PostComment comment2 = new PostComment( "Comment 2" );
		post.addComment( comment1 );
		post.addComment( comment2 );

		test(
			context,
			getSessionFactory()
				.withTransaction( session -> session.persist( post ) )
				.thenCompose( v -> getSessionFactory().withTransaction( session -> session
					.find( Post.class, post.id )
					.thenCompose( foundPost -> session
						.fetch( foundPost.comments )
						.thenApply( comments -> foundPost )
					)
					.thenAccept( foundPost -> {
						// Remove one comment - should trigger orphan removal
						PostComment toRemove = foundPost.comments.stream()
							.filter( c -> "Comment 1".equals( c.getContent() ) )
							.findFirst()
							.orElseThrow();
						foundPost.removeComment( toRemove );
					} )
				) )
				.thenCompose( v -> getSessionFactory().withTransaction( session -> session
					.find( Post.class, post.id )
					.thenCompose( foundPost -> session
						.fetch( foundPost.comments )
						.thenAccept( comments -> assertThat( comments )
							.hasSize( 1 )
							.extracting( PostComment::getContent )
							.containsExactly( "Comment 2" )
						)
					)
				) )
		);
	}

	@Test
	public void testOrphanRemovalWhenClearingComments(VertxTestContext context) {
		Post post = new Post( "Test Post" );
		post.addComment( new PostComment( "Comment 1" ) );
		post.addComment( new PostComment( "Comment 2" ) );

		test(
			context,
			getSessionFactory()
				.withTransaction( session -> session.persist( post ) )
				.thenCompose( v -> getSessionFactory().withTransaction( session -> session
					.find( Post.class, post.id )
					.thenCompose( foundPost -> session
						.fetch( foundPost.comments )
						.thenApply( comments -> foundPost )
					)
					.thenAccept( foundPost -> {
						// Clear all comments - should trigger orphan removal for all
						foundPost.clearComments();
					} )
				) )
				.thenCompose( v -> getSessionFactory().withTransaction( session -> session
					.find( Post.class, post.id )
					.thenCompose( foundPost -> session
						.fetch( foundPost.comments )
						.thenAccept( comments -> assertThat( comments ).isEmpty() )
					)
				) )
		);
	}

	/**
	 * This test reproduces the original bug scenario:
	 * 1. Load entity into persistence context
	 * 2. Execute JOIN FETCH query on the same entity
	 * 3. Modify the collection
	 * 4. Flush should not throw HibernateException
	 */
	@Test
	public void testJoinFetchDoesNotCauseOrphanException(VertxTestContext context) {
		Post post = new Post( "Test Post" );
		post.addComment( new PostComment( "Original Comment" ) );

		test(
			context,
			getSessionFactory()
				// First, persist the post with one comment
				.withTransaction( session -> session.persist( post ) )
				// Load the entity normally (creates PersistentCollection@A)
				.thenCompose( v -> getSessionFactory().withSession( session -> session
					.find( Post.class, post.id )
					.thenAccept( foundPost -> assertThat( foundPost ).isNotNull() )
				) )
				// Now execute JOIN FETCH on the same entity (original bug: creates PersistentCollection@B)
				.thenCompose( v -> getSessionFactory().withTransaction( session -> session
					.createSelectionQuery(
						"from Post p left join fetch p.comments where p.id = :id",
						Post.class
					)
					.setParameter( "id", post.id )
					.getSingleResult()
					.thenAccept( fetchedPost -> {
						// Modify the collection after JOIN FETCH
						PostComment newComment = new PostComment( "New Comment After Join Fetch" );
						fetchedPost.addComment( newComment );
						// Flush should not throw HibernateException about orphaned collection
					} )
				) )
				// Verify the modification was successful
				.thenCompose( v -> getSessionFactory().withTransaction( session -> session
					.find( Post.class, post.id )
					.thenCompose( foundPost -> session
						.fetch( foundPost.comments )
						.thenAccept( comments -> assertThat( comments )
							.hasSize( 2 )
							.extracting( PostComment::getContent )
							.containsExactlyInAnyOrder( "Original Comment", "New Comment After Join Fetch" )
						)
					)
				) )
		);
	}

	/**
	 * Another variation: clear() after JOIN FETCH
	 * This was also mentioned in the original bug report
	 */
	@Test
	public void testClearCommentsAfterJoinFetch(VertxTestContext context) {
		Post post = new Post( "Test Post" );
		post.addComment( new PostComment( "Comment 1" ) );
		post.addComment( new PostComment( "Comment 2" ) );

		test(
			context,
			getSessionFactory()
				.withTransaction( session -> session.persist( post ) )
				.thenCompose( v -> getSessionFactory().withSession( session -> session
					.find( Post.class, post.id )
					.thenAccept( foundPost -> assertThat( foundPost ).isNotNull() )
				) )
				.thenCompose( v -> getSessionFactory().withTransaction( session -> session
					.createSelectionQuery(
						"from Post p left join fetch p.comments where p.id = :id",
						Post.class
					)
					.setParameter( "id", post.id )
					.getSingleResult()
					.thenAccept( fetchedPost -> {
						// Clear after JOIN FETCH - should not throw exception
						fetchedPost.clearComments();
					} )
				) )
				.thenCompose( v -> getSessionFactory().withTransaction( session -> session
					.find( Post.class, post.id )
					.thenCompose( foundPost -> session
						.fetch( foundPost.comments )
						.thenAccept( comments -> assertThat( comments ).isEmpty() )
					)
				) )
		);
	}

	@Entity(name = "Post")
	@Table(name = "ORCT_Post")
	public static class Post {
		@Id
		@GeneratedValue
		private Long id;

		private String title;

		@OneToMany(
			mappedBy = "post",
			cascade = CascadeType.ALL,
			orphanRemoval = true,
			fetch = FetchType.LAZY
		)
		private List<PostComment> comments = new ArrayList<>();

		public Post() {
		}

		public Post(String title) {
			this.title = title;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getTitle() {
			return title;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public List<PostComment> getComments() {
			return comments;
		}

		public void setComments(List<PostComment> comments) {
			this.comments = comments;
		}

		public void addComment(PostComment comment) {
			comments.add( comment );
			comment.setPost( this );
		}

		public void removeComment(PostComment comment) {
			comments.remove( comment );
			comment.setPost( null );
		}

		public void clearComments() {
			comments.clear();
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Post post = (Post) o;
			return Objects.equals( title, post.title );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( title );
		}

		@Override
		public String toString() {
			return "Post{" +
				"id=" + id +
				", title='" + title + '\'' +
				'}';
		}
	}

	@Entity(name = "PostComment")
	@Table(name = "ORCT_PostComment")
	public static class PostComment {
		@Id
		@GeneratedValue
		private Long id;

		private String content;

		@ManyToOne(fetch = FetchType.LAZY)
		@JoinColumn(name = "post_id")
		private Post post;

		public PostComment() {
		}

		public PostComment(String content) {
			this.content = content;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getContent() {
			return content;
		}

		public void setContent(String content) {
			this.content = content;
		}

		public Post getPost() {
			return post;
		}

		public void setPost(Post post) {
			this.post = post;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			PostComment that = (PostComment) o;
			return Objects.equals( content, that.content );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( content );
		}

		@Override
		public String toString() {
			return "PostComment{" +
				"id=" + id +
				", content='" + content + '\'' +
				'}';
		}
	}
}
