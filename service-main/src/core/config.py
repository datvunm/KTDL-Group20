from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_host: str = "db"
    postgres_port: int = 5432
    postgres_user: str = "postgres"
    postgres_password: str = "123456"
    postgres_db: str = "postgres"

    mongo_host: str = "mongo"
    mongo_port: int = 27017
    mongo_user: str = "root"
    mongo_password: str = "123456"
    mongo_db: str = "airlines"

    hdfs_url: str = "http://namenode:9870"
    hdfs_user: str = "root"


settings = Settings()
