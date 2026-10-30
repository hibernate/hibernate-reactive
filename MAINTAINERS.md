# Guide for maintainers of Hibernate Reactive

This guide is intended for maintainers of Hibernate Reactive,
i.e. anybody with direct push access to the git repository.

## <a id="releasing"></a> Releasing

Releases are performed by the release job on CI, with the scripts of
[hibernate-release-scripts](https://github.com/hibernate/hibernate-release-scripts).

### Preparing the release

Before the release:

* Check that everything has been pushed to the upstream repository,
  and that the CI jobs for the branch you want to release are green.
* Check the [milestone](https://github.com/hibernate/hibernate-reactive/milestones) of the release:
  * It must have the name of the release tag, so that the release closes it:
    `5.1.0.Beta1` for a pre-release, and `4.5.13` for `4.5.13.Final`.
  * Assign the fixed issues to it, and move the open ones to another milestone.
* If it's the first release of a new major or minor version, check that the
  [migration guide](migration-guide.adoc) is up to date.
* If you want highlights in the GitHub release notes, add them to
  [github_release_notes.md](github_release_notes.md) and push it to the branch you want to release:
  the release job uses it as the body of the "What's new" section of the GitHub release.

### Performing the release

* Trigger the release job on CI for the branch you want to release.
  Only `RELEASE_VERSION` is mandatory, e.g. `5.1.0.Final`.
  Use `RELEASE_DRY_RUN` to check the release without publishing anything.
* The job updates [hibernate.org](https://github.com/hibernate/hibernate.org):
  it adds the release and, for the first release of a series, the `series.yml` of the series
  that the build generates in `build/hibernate.org` (the task `websiteSeriesFile`).
  Later changes to `series.yml`, e.g. the compatible versions, are done by hand on hibernate.org.
  If the job warns that `series.yml` doesn't exist, add it by hand: hibernate.org won't build without it.

### After the release

* Check the [GitHub release](https://github.com/hibernate/hibernate-reactive/releases) notes.
  If you added highlights to [github_release_notes.md](github_release_notes.md), remove them,
  otherwise they will appear in the notes of the next release too.
* Update the default versions in the [JBang templates](tooling/jbang) to the new release,
  once the artifacts are on Maven Central: for example,
  `//DEPS org.hibernate.reactive:hibernate-reactive-core:${hibernate-reactive.version:5.1.0.Beta1}`,
  and the Vert.x version if it changed.
* Check the page of the series on hibernate.org, e.g. https://hibernate.org/reactive/releases/5.1/.
* Check that the artifacts are available on
  [Maven Central](https://central.sonatype.com/artifact/org.hibernate.reactive/hibernate-reactive-core).
  They should appear after a few minutes, sometimes a few hours.

### Announcing the release

If it's an `Alpha`, `Beta`, `CR` or first `Final` (`x.y.0.Final`) release, announce it
on [in.relation.to](https://github.com/hibernate/in.relation.to), with the tags "Hibernate Reactive" and "Releases":

* Write the post in `posts/<author>/<date>-hibernate-reactive-<version>.adoc`,
  with the versions of Hibernate ORM and Vert.x, the highlights and the links to the milestones,
  then open a pull request on in.relation.to.
* When you release several versions at the same time, a single post can announce them.
* Once the post is published, add its URL to the release files on hibernate.org,
  e.g. `_data/projects/reactive/releases/5.1/5.1.0.Beta1.yml`:
  ```yaml
  announcement_url: https://in.relation.to/2026/10/30/hibernate-reactive-5_1_0_Beta1/
  ```
  The nightly check of hibernate.org reports the releases without an announcement.
