import logging
from typing import Awaitable, Callable

import aiomysql
import neo4j
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.sdk.resources import (
    DEPLOYMENT_ENVIRONMENT,
    SERVICE_NAME,
    TELEMETRY_SDK_LANGUAGE,
    Resource,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import set_tracer_provider

from knowledge_complex_backend.services.redis.lifetime import init_redis, shutdown_redis
from knowledge_complex_backend.settings import settings


def setup_opentelemetry(app: FastAPI) -> None:  # pragma: no cover
    """
    Enables opentelemetry instrumentation.

    :param app: current application.
    """
    if not settings.opentelemetry_endpoint:
        return

    tracer_provider = TracerProvider(
        resource=Resource(
            attributes={
                SERVICE_NAME: "knowledge_complex_backend",
                TELEMETRY_SDK_LANGUAGE: "python",
                DEPLOYMENT_ENVIRONMENT: settings.environment,
            },
        ),
    )

    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=settings.opentelemetry_endpoint,
                insecure=True,
            ),
        ),
    )

    excluded_endpoints = [
        app.url_path_for("health_check"),
        app.url_path_for("openapi"),
        app.url_path_for("swagger_ui_html"),
        app.url_path_for("swagger_ui_redirect"),
        app.url_path_for("redoc_html"),
    ]

    FastAPIInstrumentor().instrument_app(
        app,
        tracer_provider=tracer_provider,
        excluded_urls=",".join(excluded_endpoints),
    )
    RedisInstrumentor().instrument(
        tracer_provider=tracer_provider,
    )

    set_tracer_provider(tracer_provider=tracer_provider)


def stop_opentelemetry(app: FastAPI) -> None:  # pragma: no cover
    """
    Disables opentelemetry instrumentation.

    :param app: current application.
    """
    if not settings.opentelemetry_endpoint:
        return

    FastAPIInstrumentor().uninstrument_app(app)
    RedisInstrumentor().uninstrument()


async def _setup_db(app: FastAPI) -> None:  # pragma: no cover
    """
    Creates connection to the database.

    This function creates SQLAlchemy engine instance,
    session_factory for creating sessions
    and stores them in the application's state property.

    :param app: fastAPI application.
    """
    from urllib.parse import urlsplit

    result = urlsplit(str(settings.db_url))
    logging.info("db uri: %s", result)
    app.state.mysql_pool = await aiomysql.create_pool(
        host=result.hostname,
        port=result.port,
        user=result.username,
        password=result.password,
        db=result.path.strip("/"),
    )

    # todo set in emv
    # app.state.gpc_mysql_pool = await aiomysql.create_pool(
    #     host="192.168.1.229",
    #     port=3329,
    #     user="root",
    #     password="root",
    #     db="gpc",
    # )

    app.state.wikipedia_elastic_client = AsyncElasticsearch("http://192.168.1.227:9200")
    app.state.neo4j_driver = neo4j.AsyncGraphDatabase.driver(
        "bolt://192.168.1.229:17689",
        auth=("neo4j", "neo4j-test"),
    )
    client = AsyncIOMotorClient("mongodb://knogen:knogen@192.168.1.227:27017")
    app.state.mongo_database = client.get_database("knogen_complex_backend")


def register_startup_event(
    app: FastAPI,
) -> Callable[[], Awaitable[None]]:  # pragma: no cover
    """
    Actions to run on application startup.

    This function uses fastAPI app to store data
    in the state, such as db_engine.

    :param app: the fastAPI application.
    :return: function that actually performs actions.
    """

    @app.on_event("startup")
    async def _startup() -> None:  # noqa: WPS430
        setup_opentelemetry(app)
        init_redis(app)
        await _setup_db(app)
        pass  # noqa: WPS420

    return _startup


def register_shutdown_event(
    app: FastAPI,
) -> Callable[[], Awaitable[None]]:  # pragma: no cover
    """
    Actions to run on application's shutdown.

    :param app: fastAPI application.
    :return: function that actually performs actions.
    """

    @app.on_event("shutdown")
    async def _shutdown() -> None:  # noqa: WPS430
        await shutdown_redis(app)
        stop_opentelemetry(app)

        app.state.mysql_pool.close()
        await app.state.mysql_pool.wait_closed()

        # app.state.gpc_mysql_pool.close()
        # await app.state.gpc_mysql_pool.wait_closed()

        await app.state.wikipedia_elastic_client.close()
        await app.state.neo4j_driver.close()

        pass  # noqa: WPS420

    return _shutdown
