init:
	# conda activate py311
	export PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring
	echo "init"

poetry:
	curl -sSL https://install.python-poetry.org | python3 -

dev:init
	poetry run python -m knowledge_complex_backend

install:init
	poetry install
