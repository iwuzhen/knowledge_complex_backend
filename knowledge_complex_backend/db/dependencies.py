
from starlette.requests import Request


async def get_db_pool(
    request: Request,
) -> any:
    """
    Create and get database session.

    :param request: current request.
    :yield: database session.
    """
    return request.app.state.mysql_pool

    # try:  # noqa: WPS501
    #     yield session
    # finally:
    #     await session.commit()
    #     await session.close()


async def get_gpc_db_pool(
    request: Request,
) -> any:
    """
    Create and get database session.

    :param request: current request.
    :yield: database session.
    """
    return request.app.state.gpc_mysql_pool


async def get_wikipedia_es_client(
    request: Request,
) -> any:
    """
    Create and get database session.

    :param request: current request.
    :yield: database session.
    """
    return request.app.state.wikipedia_elastic_client


async def get_neo4j_driver(
    request: Request,
) -> any:
    """
    Create and get database session.

    :param request: current request.
    :yield: database session.
    """
    return request.app.state.neo4j_driver
