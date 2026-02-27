# Contributing

Contributions from the community are essential in keeping Hibernate (and any Open Source
project really) strong and successful.  

# Legal

All original contributions to Hibernate are licensed under the 
[Apache 2.0 license](https://www.apache.org/licenses/LICENSE-2.0).

The Apache 2.0 license text is included verbatim in the [LICENSE](LICENSE) file in the root directory
of the Hibernate Reactive repository.

All contributions are subject to the [Developer Certificate of Origin (DCO)](https://developercertificate.org/).  

The DCO text is available verbatim in the [dco.txt](dco.txt) file in the root directory
of the Hibernate Reactive repository.

## Guidelines

While we try to keep requirements for contributing to a minimum, there are a few guidelines 
we ask that you mind.

For code contributions, these guidelines include:
* Respect the project code style - find templates for [IntelliJ IDEA](https://hibernate.org/community/contribute/intellij-idea/) or [Eclipse](https://hibernate.org/community/contribute/eclipse-ide/)
* Have a corresponding GitHub [issue](https://github.com/hibernate/hibernate-reactive/issues) and be sure to include
  the key for this issue in your commit messages.
* Have a set of appropriate tests.  
  For your convenience, a [set of test templates](https://github.com/hibernate/hibernate-test-case-templates/tree/main/reactive)
  have been made available.
  	
  When submitting bug reports, the tests should reproduce the initially reported bug and illustrate that your solution addresses the issue.
  For features/enhancements, the tests should demonstrate that the feature works as intended.  
  In both cases, be sure to incorporate your tests into the project to protect against possible regressions.
* If applicable, documentation should be updated to reflect the introduced changes
* The code compiles and the tests pass (`./gradlew clean build`)

For documentation contributions, mainly to respect the project code style, especially in regard 
to the use of tabs - as mentioned above, code style templates are available for both IntelliJ IDEA and Eclipse
IDEs.  Ideally, these contributions would also have a corresponding issue, although this 
is less necessary for documentation contributions.

## Getting Started

If you are just getting started with Git, GitHub, and/or contributing to Hibernate via
GitHub there are a few pre-requisite steps to follow:

* Make sure you have a [GitHub account](https://github.com/signup/free)
* [Fork](https://help.github.com/articles/fork-a-repo) the Hibernate Reactive repository.  As discussed in
the linked page, this also includes:
    * [set up your local git install](https://help.github.com/articles/set-up-git) 
    * clone your fork
* Instruct git to ignore certain commits when using `git blame`. From the directory of your local clone, run this: `git config blame.ignoreRevsFile .git-blame-ignore-revs`
* See the wiki pages for setting up your IDE, whether you use 
[IntelliJ IDEA](https://hibernate.org/community/contribute/intellij-idea/)
or [Eclipse](https://hibernate.org/community/contribute/eclipse-ide/)<sup>(1)</sup>.

## Testing with a local Hibernate ORM build

When working on issues that require changes in both Hibernate ORM and Hibernate Reactive,
you can use a local Hibernate ORM repository as a dependency without publishing artifacts locally.

To enable this feature, set the `localHibernateOrmPath` property to point to your local Hibernate ORM clone:

```bash
./gradlew build -PlocalHibernateOrmPath=../hibernate-orm
```

Alternatively, add this to your `gradle.properties` file:

```properties
localHibernateOrmPath = ../hibernate-orm
```

This uses Gradle's composite build feature, which means:
* Changes in Hibernate ORM are automatically picked up without publishing
* It works seamlessly in IntelliJ IDEA
* You can iterate quickly on changes across both projects

## Create the working (topic) branch

Create a [topic branch](https://git-scm.com/book/en/Git-Branching-Branching-Workflows#Topic-Branches) 
on which you will work.  The convention is to incorporate the JIRA issue key in the name of this branch,
although this is more of a mnemonic strategy than a hard-and-fast rule - but doing so helps:
* Remember what each branch is for 
* Isolate the work from other contributions you may be working on

_If there is not already a GitHub issue covering the work you want to do, [create one](https://github.com/hibernate/hibernate-reactive/issues/new)._
  
Assuming you will be working from the `main` branch and working
on the GitHub issue #123 : `git checkout -b 123 main`

## Code

Do your thing!


## Commit

* Make commits of logical units
* Be sure to start each commit message using the ** GitHub issue key **. For example:
  ```
  [#1234] Fix some kind of problem
  ```
* Make sure you have added the necessary tests for your changes
* Run _all_ the tests to ensure nothing else was accidentally broken

_Before committing, if you want to pull in the latest upstream changes (highly
appreciated btw), please use rebasing rather than merging.  Merging creates
"merge commits" that invariably muck up the project timeline._

## Submit

* Push your changes to the topic branch in your fork of the repository
* Initiate a [pull request](https://help.github.com/articles/creating-a-pull-request)
* Adding the sentence `Fix #123`, where `#123` is the issue key, will link the pull request to the corresponding issue
  and close it accordingly, when the pull request gets merged.

It is important that this topic branch of your fork:

* Is isolated to just the work on this one issue, or multiple issues if they are
  related and also fixed/implemented by this work.  The main point is to not push commits for more than
  one PR to a single branch - GitHub PRs are linked to a branch rather than specific commits
* remain until the PR is closed. Once the underlying branch is deleted the corresponding PR will be closed,
  if not already, and the changes will be lost.

# Notes
<sup>(1)</sup> Gradle `eclipse` plugin is no longer supported, so the recommended way to import the project in your IDE is with the proper IDE tools/plugins. Don't try to run `./gradlew clean eclipse --refresh-dependencies` from the command line as you'll get an error because `eclipse` no longer exists
