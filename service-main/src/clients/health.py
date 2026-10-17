from src.clients import hdfs, mongo, postgres

def check_postgres() -> dict:
    try:
        conn = postgres.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            return {"service": "postgres", "ok": True, "output": cur.fetchone()[0]}
        finally:
            conn.close()
    except Exception as e:
        return {"service": "postgres", "ok": False, "error": str(e)}


def check_mongo() -> dict:
    try:
        client = mongo.get_client()
        return {"service": "mongo", "ok": True, "output": client.admin.command("ping")}
    except Exception as e:
        return {"service": "mongo", "ok": False, "error": str(e)}


def check_hdfs() -> dict:
    try:
        client = hdfs.get_client()
        return {"service": "hdfs", "ok": True, "output": client.list("/")}
    except Exception as e:
        return {"service": "hdfs", "ok": False, "error": str(e)}


def check_all() -> dict:
    results = [check_postgres(), check_mongo(), check_hdfs()]
    return {"ok": all(r["ok"] for r in results), "services": results}
