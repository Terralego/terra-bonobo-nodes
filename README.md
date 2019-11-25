# terra-bonobo-nodes

This package provides a set of bonobo ETL's nodes to help developpers to
integrate data in a terralego based app.

## To start a dev instance

Define settings you wants in `tests/sample_project/projecttest/` django project.

```sh
docker-compose up
```

## Test

To run test suite, just launch:

```sh
docker-compose run web /code/venv/bin/python3 /code/src/manage.py test
```

Run every tests
```
docker-compose run --rm django bash
./venv/bin/tox -c tox.ini -e linting
./venv/bin/tox -c tox.ini -e coverage
./venv/bin/tox -c tox.ini -e tests
```

Note for osx users, use ``docker-compose``  this way
```
docker-compose -f docker-compose.yml -f docker-compose-osx.yml $args
```
