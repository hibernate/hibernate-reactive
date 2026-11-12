# Guidelines for AI coding agents

These guidelines apply to any change made with an AI coding agent (Claude Code, Codex, ...),
and to the contributors who review it. They complement [CONTRIBUTING.md](CONTRIBUTING.md),
which still applies.

These are guidelines rather than strict rules: follow them by default,
but use your judgment when one doesn't fit the change, and say so in the pull request.
The exceptions are the rules marked **always**, which have no exceptions.

If you delegate work to other agents, pass these guidelines on to them.

When you help with a release, follow the [release checklist](https://github.com/hibernate/hibernate-reactive/blob/main/MAINTAINERS.md#releasing).

## Commits

- Start the title with the GitHub issue key: `[#1234] Fix some kind of problem`.
- Keep the title at most 72 characters, ideally about 60, including the issue key.
  GitHub truncates longer titles when it uses them as the pull request title.
- Put the fix and the tests in separate commits, the fix first and the tests after it.
- Put the changes that don't affect the compilation or the build, like the documentation or the migration guide,
  in separate commits from the code changes.
- **Always** make every commit compile and pass the build on its own, so that the history stays bisectable.
- When you take over a contributor's pull request, reorder or squash its commits as needed
  to keep every commit building, for example by folding a compilation fix into the commit that broke it.
  Credit the original author with a `Co-authored-by: Name <email>` trailer.
- **Always** credit the agent with an `Assisted-by:` trailer that includes the model and its version,
  for example `Assisted-by: Claude Opus 5.5 <noreply@anthropic.com>`. Don't use `Co-Authored-By:` for the agent.
- **Always** ask a maintainer before you push, open or comment on pull requests, or create issues.
  Local commits are fine: a maintainer reviews them first.

## Pull requests

- Start the title with the version family of the target branch, in brackets:
  `[5.1] Fix some kind of problem`. The family is the major and minor version in
  `gradle/version.properties` of that branch, for example `5.1` for `5.1.0-SNAPSHOT`.
- Add the label of the same family, for example `5.1`.
- Add the other existing labels that describe the change, for example `bug`, `enhancement`,
  `regression`, `testing`, `documentation`, `upgrade`, `build`, `ci` or `performance`,
  the database it affects (`db2`, `oracle`, `cockroachdb`), or `quarkus` when it is linked to a Quarkus issue.
  The labels of the issue are a good starting point. Don't create new labels.
- Set the milestone of the next release of that family. Milestones have the name of the release tag,
  so that the release closes them: `5.1.0.Beta1` for a pre-release, and `4.5.13` for `4.5.13.Final`.
- When a change goes to more than one branch, open a pull request for each branch,
  with its own family, label and milestone: `[5.1] Fix some kind of problem` on `main`
  and `[5.0] Fix some kind of problem` on `5.0`.
- Start the description of the pull request on `main` with `Fixes #1234`, so that merging it closes the issue.
  For the other branches, start it with ``Backport #1234 (#1235) to `5.0` ``, with the issue and the pull request on `main`:
  `Fixes` only closes issues when merged into `main`.

## Tests

- Create the expected entity instances once and reuse them for the operation and the assertion,
  instead of creating a new instance inside each assertion.
- Implement `equals`/`hashCode` on the test entities and assert with `assertThat( found ).isEqualTo( expected )`,
  instead of adding custom `assertXxx` helper methods.
- Don't include the id in `equals`/`hashCode` of the test entities.
- Don't link the issue in the Javadoc of a test (`@see .../issues/1234`): the commit message already references it.
- When a new test looks like an existing one, add a short comment saying what it covers that the other one doesn't.
  Avoid step-by-step comments inside the test.
- A test for a bug should fail without the fix: check it before committing.

## Building and testing

- **Always** run `./gradlew spotlessApply` before committing. The build checks the formatting.
- Select the database with `-Pdb`, for example `./gradlew :hibernate-reactive-core:test -Pdb=pg --tests "*SessionTest*"`.
  The supported values and the dedicated tasks are listed in the [README](README.md).
  The tests start the database with Testcontainers; see [podman.md](podman.md) to start it yourself.
- With rootless Podman instead of Docker, start the Podman socket and set `DOCKER_HOST` for Testcontainers:
  see [Running the tests with Testcontainers and Podman](podman.md#running-the-tests-with-testcontainers-and-podman).
  An error about Ryuk usually means that Podman cannot start containers at all:
  check with `podman run` before disabling Ryuk.
- Changes in shared code (the connection pool, the session, the query execution) should be tested
  on more than one database, at least PostgreSQL and MySQL or MariaDB.
