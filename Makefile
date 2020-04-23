.PHONY: install-dev lint test cover wheel wheel-lint

	install-master:
		pip install -r requirements.txt

	lint:
		flake8

	wheel:
		python setup.py sdist bdist_wheel

	wheel-lint: wheel
		twine check dist/*
