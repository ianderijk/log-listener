format:
	uv run ruff format .

lint:
	uv run ruff check .

typecheck:
	uv run ty check

check: format lint typecheck

build:
	uv build

publish:
	uv publish --index home-index
