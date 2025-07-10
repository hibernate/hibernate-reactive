Our test suite will only read the first FROM instruction from each Dockerfile to extract the base image and the version
of the container to run. It will ignore everything else.

The reason we have these files is that we want to automate the upgrade of the containers using dependabot.

See the class `DockerImage`.
