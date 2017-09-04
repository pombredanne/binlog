FROM themattrix/tox-base
COPY requirements-*.txt README.rst CHANGELOG.rst setup.py MANIFEST.in tox.ini /app/
COPY binlog /app/binlog
COPY tests /app/tests
COPY docs /app/docs
VOLUME /app/.tox
VOLUME /app/htmlcov
