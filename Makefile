test:
	docker build . -t binlog
	docker run -ti --rm -v $(PWD)/.tox:/app/.tox -v $(PWD)/htmlcov:/app/htmlcov binlog
