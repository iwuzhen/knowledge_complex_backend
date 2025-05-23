init:
	# conda activate py311
	export PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring
	echo "init"


dev:init
	uv run python -m knowledge_complex_backend

install:init
	uv install
