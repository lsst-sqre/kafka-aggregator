.PHONY: update-deps
update-deps:
	pip install --upgrade pip-tools pip setuptools
	pip-compile --upgrade --build-isolation --generate-hashes --output-file requirements/main.txt requirements/main.in
	pip-compile --upgrade --build-isolation --generate-hashes --output-file requirements/dev.txt requirements/dev.in requirements/main.in
	pip-sync requirements/main.txt requirements/dev.txt

.PHONY: init
init:
	pip install --editable .
	pip install --upgrade -r requirements/main.txt -r requirements/dev.txt
	rm -rf .tox
	pip install --upgrade tox
	pre-commit install

.PHONY: update
update: update-deps init

.PHONY: run
run:
	kafkaaggregator
