# terra-bonobo-nodes

This package provides a set of bonobo ETL's nodes to help developpers to
integrate data in a terralego based app.

## To start a dev instance

Define settings you wants in `tests/sample_project/projecttest/` django project.

```sh
docker-compose up
```

## Test

To run test suite, while stack up, just launch:

```sh
docker-compose exec django /app/venv/bin/tox -e tests
```

Run linting, while stack up:

```
docker-compose exec django /app/venv/bin/tox -e linting
```

Note for osx users, use ``docker-compose``  this way
```
docker-compose -f docker-compose.yml -f docker-compose-osx.yml $args
```
