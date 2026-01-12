install:
	poetry install

database:
	poetry run database

build:
	poetry build

publish:
	poetry publish --dry-run

package-install: build
	python3 -m pip install dist/*.whl

lint:
	poetry run ruff check .