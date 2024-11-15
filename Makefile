install:
	pip install --upgrade pip && \
		pip3 install -r requirements.txt

test:
	python3 -m pytest -vv --cov=mylib test_*.py

format:
	black *.py 

lint:
	ruff check *.py mylib/*.py

container-lint:
	docker run --rm -i hadolint/hadolint < Dockerfile

refactor: format lint

deploy:
	# Custom deploy command goes here

all: install lint test format deploy


