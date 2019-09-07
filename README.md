# terra-bonobo-nodes

This package provides a set of bonobo ETL's nodes to help developpers to
integrate data in a terralego based app.


## test with docker

build
```
docker-compose build
```

launch pgsql
```
docker-compose up -d db
docker-compose logs db  # verify everything is ok
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
