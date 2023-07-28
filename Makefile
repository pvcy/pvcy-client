.PHONY: check
check:
	black .
	ruff . --fix
	pytest tests/unit_tests
	mypy

.PHONY: full
full:
	black .
	ruff . --fix
	pytest
	mypy
