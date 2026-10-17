from fastapi import APIRouter

from src.clients import health

router = APIRouter(prefix="/health", tags=["health"])


@router.get("")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/postgres")
def health_postgres() -> dict:
    return health.check_postgres()


@router.get("/mongo")
def health_mongo() -> dict:
    return health.check_mongo()



@router.get("/hdfs")
def health_hdfs() -> dict:
    return health.check_hdfs()


@router.get("/all")
def health_all() -> dict:
    return health.check_all()
