import uvicorn

from knowledge_complex_backend.settings import settings


def main() -> None:
    """Entrypoint of the application."""
    uvicorn.run(
        "knowledge_complex_backend.web.application:get_app",
        workers=settings.workers_count,
        host=settings.host,
        port=settings.port,
        reload=settings.reload,
        log_level=settings.log_level.value.lower(),
        factory=True,
    )


# find max a
if __name__ == "__main__":
    main()
