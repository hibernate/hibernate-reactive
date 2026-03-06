Each Dockerfile contains the full container configuration for a database:
the base image, environment variables, exposed ports, and startup commands.

They serve two purposes:

1. **Testcontainers**: The test suite only reads the `FROM` instruction to extract the base image
   name and version. Everything else (ENV, EXPOSE, CMD) is ignored by Testcontainers.
   See the class `DockerImage`.

2. **Manual testing**: You can build and run them directly with Podman or Docker to start a database
   without Testcontainers. In this case, the full Dockerfile is used.
   See `podman.md` in the project root for instructions.

Image versions are kept up-to-date automatically by Dependabot.
