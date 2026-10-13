"""Static container and Compose contracts for the local mock API."""

from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = REPOSITORY_ROOT / "compose.yaml"
DOCKERFILE_PATH = REPOSITORY_ROOT / "mock_api" / "Dockerfile"
DOCKERIGNORE_PATH = REPOSITORY_ROOT / ".dockerignore"
ENV_EXAMPLE_PATH = REPOSITORY_ROOT / ".env.example"


def test_mock_api_uses_the_pinned_python_image_and_no_runtime_dependency_install() -> None:
    dockerfile = DOCKERFILE_PATH.read_text(encoding="utf-8")

    assert dockerfile.splitlines()[0] == "FROM python:3.12.12-slim-bookworm"
    assert 'CMD ["python", "-m", "mock_api.server"]' in dockerfile
    assert "pip install" not in dockerfile
    assert "uv " not in dockerfile
    assert "latest" not in dockerfile


def test_compose_wires_mock_api_seed_port_and_bounded_healthcheck() -> None:
    compose = COMPOSE_PATH.read_text(encoding="utf-8")

    assert "  mock-api:" in compose
    assert "dockerfile: mock_api/Dockerfile" in compose
    assert "SOURCE_SEED: ${SOURCE_SEED:-20260901}" in compose
    assert '"127.0.0.1:${MOCK_API_PORT:-8080}:8080"' in compose
    assert compose.count("healthcheck:") == 3
    assert "http://localhost:8080/health" in compose
    assert compose.count("timeout: 5s") == 3
    assert compose.count("retries: 12") == 3
    assert "depends_on:" not in compose


def test_example_environment_publishes_non_secret_mock_configuration() -> None:
    env_example = ENV_EXAMPLE_PATH.read_text(encoding="utf-8")

    assert "SOURCE_SEED=20260901" in env_example
    assert "MOCK_API_PORT=8080" in env_example
    assert "MOCK_API_BASE_URL=http://localhost:8080" in env_example


def test_docker_build_context_excludes_local_secrets_and_generated_state() -> None:
    dockerignore = DOCKERIGNORE_PATH.read_text(encoding="utf-8").splitlines()

    assert ".env" in dockerignore
    assert ".env.*" in dockerignore
    assert ".git" in dockerignore
    assert ".venv" in dockerignore
    assert "__pycache__" in dockerignore
    assert "fixtures/generated" in dockerignore
