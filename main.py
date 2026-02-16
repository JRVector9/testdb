import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import OperationalError
import redis

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:vector9devdb@192.168.139.217:5432/postgres",
)
REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://:vector9redis@192.168.139.111:6379",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=5)
redis_client = redis.from_url(REDIS_URL, decode_responses=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(title="DB Connection Test", lifespan=lifespan)


# ─── Health ───────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "service": "testdb"}


# ─── PostgreSQL 연결 테스트 ────────────────────────────────
@app.get("/pg/connect")
def pg_connect():
    """PostgreSQL 연결 테스트"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            return {"status": "connected", "version": version}
    except OperationalError as e:
        raise HTTPException(status_code=500, detail=f"PG connection failed: {e}")


# ─── PostgreSQL 테이블 생성 테스트 ─────────────────────────
@app.post("/pg/create-table")
def pg_create_table():
    """테스트 테이블 생성"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS test_connection (
                    id SERIAL PRIMARY KEY,
                    message TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """))
            conn.commit()
            return {"status": "ok", "message": "test_connection table created"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── PostgreSQL 데이터 삽입 테스트 ─────────────────────────
@app.post("/pg/insert")
def pg_insert(message: str = "hello from testdb"):
    """데이터 삽입 테스트"""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("INSERT INTO test_connection (message) VALUES (:msg) RETURNING id, message, created_at"),
                {"msg": message},
            )
            row = result.fetchone()
            conn.commit()
            return {"status": "inserted", "id": row[0], "message": row[1], "created_at": str(row[2])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── PostgreSQL 데이터 읽기 테스트 ─────────────────────────
@app.get("/pg/read")
def pg_read():
    """데이터 읽기 테스트"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT id, message, created_at FROM test_connection ORDER BY id DESC LIMIT 10"))
            rows = [{"id": r[0], "message": r[1], "created_at": str(r[2])} for r in result.fetchall()]
            return {"status": "ok", "count": len(rows), "rows": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── PostgreSQL 새 DB 생성 테스트 ──────────────────────────
@app.post("/pg/create-db")
def pg_create_db(db_name: str = "test_newdb"):
    """새 데이터베이스 생성 가능한지 테스트"""
    try:
        # autocommit이 필요하므로 raw connection 사용
        raw_conn = engine.raw_connection()
        raw_conn.set_isolation_level(0)  # AUTOCOMMIT
        cursor = raw_conn.cursor()
        # DB 존재 여부 확인
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        exists = cursor.fetchone()
        if exists:
            cursor.close()
            raw_conn.close()
            return {"status": "already_exists", "db_name": db_name}
        cursor.execute(f'CREATE DATABASE "{db_name}"')
        cursor.close()
        raw_conn.close()
        return {"status": "created", "db_name": db_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── PostgreSQL 새 DB 삭제 (정리용) ───────────────────────
@app.delete("/pg/drop-db")
def pg_drop_db(db_name: str = "test_newdb"):
    """테스트 DB 삭제 (정리용)"""
    if db_name in ("openLLM", "postgres"):
        raise HTTPException(status_code=400, detail="Cannot drop protected database")
    try:
        raw_conn = engine.raw_connection()
        raw_conn.set_isolation_level(0)
        cursor = raw_conn.cursor()
        cursor.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
        cursor.close()
        raw_conn.close()
        return {"status": "dropped", "db_name": db_name}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── PostgreSQL 테이블 목록 ────────────────────────────────
@app.get("/pg/tables")
def pg_tables():
    """현재 DB의 테이블 목록"""
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return {"status": "ok", "tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Redis 연결 테스트 ─────────────────────────────────────
@app.get("/redis/connect")
def redis_connect():
    """Redis 연결 테스트"""
    try:
        pong = redis_client.ping()
        info = redis_client.info("server")
        return {
            "status": "connected",
            "ping": pong,
            "redis_version": info.get("redis_version"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis connection failed: {e}")


# ─── Redis 읽기/쓰기 테스트 ────────────────────────────────
@app.post("/redis/write")
def redis_write(key: str = "test_key", value: str = "hello_redis"):
    """Redis 쓰기 테스트"""
    try:
        redis_client.set(key, value, ex=300)  # 5분 TTL
        return {"status": "written", "key": key, "value": value, "ttl": 300}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/redis/read")
def redis_read(key: str = "test_key"):
    """Redis 읽기 테스트"""
    try:
        value = redis_client.get(key)
        ttl = redis_client.ttl(key)
        return {"status": "ok", "key": key, "value": value, "ttl": ttl}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── 전체 테스트 (한번에 실행) ─────────────────────────────
@app.get("/test-all")
def test_all():
    """PostgreSQL + Redis 전체 연결 테스트"""
    results = {}

    # PG 연결
    try:
        with engine.connect() as conn:
            ver = conn.execute(text("SELECT version()")).scalar()
            results["pg_connect"] = {"status": "ok", "version": ver}
    except Exception as e:
        results["pg_connect"] = {"status": "error", "detail": str(e)}

    # PG 읽기/쓰기
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS test_connection (
                    id SERIAL PRIMARY KEY,
                    message TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """))
            conn.execute(
                text("INSERT INTO test_connection (message) VALUES (:msg)"),
                {"msg": f"test_all at {time.time()}"},
            )
            conn.commit()
            count = conn.execute(text("SELECT count(*) FROM test_connection")).scalar()
            results["pg_readwrite"] = {"status": "ok", "row_count": count}
    except Exception as e:
        results["pg_readwrite"] = {"status": "error", "detail": str(e)}

    # Redis 연결
    try:
        pong = redis_client.ping()
        redis_client.set("testall_key", "testall_value", ex=60)
        val = redis_client.get("testall_key")
        results["redis"] = {"status": "ok", "ping": pong, "read_back": val}
    except Exception as e:
        results["redis"] = {"status": "error", "detail": str(e)}

    return results
