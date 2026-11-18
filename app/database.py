"""SQLite 持久化层。

- cells：导入的调查格值（导入后不可更换，无更新/删除接口）。
  batch 标记来源批次：0 为首次导入，>=1 为晚到格值 CSV 追加批次。
- publications：已获准公开的汇总记录（不可撤销，只插入，无更新/删除接口）。
  cell_keys 快照发布当时实际涵盖的格（JSON [["分区","年龄组"], ...]）：
  晚到追加的新格即使落在旧记录勾选的分区/年龄组范围内，也不属于旧记录，
  旧记录的总数与约束永远按该快照解释，不随后续格集合变化而改写。

所有写操作均在单事务中完成：失败即整体回滚，不会留下半批格值或半次发布。
连接按操作创建，启用 WAL，重启后数据保留在 DB_PATH 指定的库文件中。
"""

import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

_DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "app.db"
DB_PATH = os.environ.get("DB_PATH", str(_DEFAULT_DB))

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cells (
    district   TEXT NOT NULL,
    age_group  TEXT NOT NULL,
    population INTEGER NOT NULL CHECK (population >= 5),
    detected   INTEGER NOT NULL CHECK (detected >= 0 AND detected <= population),
    batch      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (district, age_group)
);
CREATE TABLE IF NOT EXISTS publications (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    districts  TEXT NOT NULL,          -- JSON 数组
    age_groups TEXT NOT NULL,          -- JSON 数组
    cell_keys  TEXT NOT NULL DEFAULT '[]',  -- JSON [["分区","年龄组"], ...] 发布当时涵盖的格
    total      INTEGER NOT NULL CHECK (total >= 0),
    created_at TEXT NOT NULL           -- ISO8601 UTC
);
"""


def connect() -> sqlite3.Connection:
    path = DB_PATH
    if path != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def init_db() -> None:
    """建表并对旧版库做前向迁移（仅加列/回填，绝不改写既有记录数值）。"""
    with closing(connect()) as conn, conn:
        conn.executescript(_SCHEMA)
        cell_cols = _table_columns(conn, "cells")
        if "batch" not in cell_cols:
            conn.execute("ALTER TABLE cells ADD COLUMN batch INTEGER NOT NULL DEFAULT 0")
        pub_cols = _table_columns(conn, "publications")
        if "cell_keys" not in pub_cols:
            conn.execute(
                "ALTER TABLE publications ADD COLUMN cell_keys TEXT NOT NULL DEFAULT '[]'"
            )
        # 回填旧记录快照：发布当时不存在晚到格，旧记录涵盖的就是当时选择交集
        # 在当前格集合中的格；回填仅补快照，total/districts/age_groups 原样保留。
        cells = conn.execute(
            "SELECT district, age_group FROM cells"
        ).fetchall()
        cell_pairs = [(r["district"], r["age_group"]) for r in cells]
        for r in conn.execute(
            "SELECT id, districts, age_groups, cell_keys FROM publications"
        ).fetchall():
            if r["cell_keys"] and json.loads(r["cell_keys"]):
                continue
            ds = set(json.loads(r["districts"]))
            ags = set(json.loads(r["age_groups"]))
            keys = [[d, a] for d, a in cell_pairs if d in ds and a in ags]
            conn.execute(
                "UPDATE publications SET cell_keys = ? WHERE id = ?",
                (json.dumps(keys, ensure_ascii=False), r["id"]),
            )


def get_cells() -> list[dict]:
    with closing(connect()) as conn:
        rows = conn.execute(
            "SELECT district, age_group, population, detected, batch FROM cells "
            "ORDER BY district, age_group"
        ).fetchall()
    return [dict(r) for r in rows]


def insert_cells(rows: list[dict], batch: int = 0) -> None:
    """单事务写入全部格值；任一失败则整体回滚。组合重复抛 IntegrityError。"""
    payload = [dict(r, batch=batch) for r in rows]
    with closing(connect()) as conn, conn:
        conn.executemany(
            "INSERT INTO cells (district, age_group, population, detected, batch) "
            "VALUES (:district, :age_group, :population, :detected, :batch)",
            payload,
        )


def next_batch_number() -> int:
    """下一追加批次号（首次导入固定为 0，追加从 1 起）。"""
    with closing(connect()) as conn:
        row = conn.execute("SELECT COALESCE(MAX(batch), 0) AS m FROM cells").fetchone()
    return int(row["m"]) + 1


def get_publications() -> list[dict]:
    with closing(connect()) as conn:
        rows = conn.execute(
            "SELECT id, districts, age_groups, cell_keys, total, created_at "
            "FROM publications ORDER BY id"
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["districts"] = json.loads(d["districts"])
        d["age_groups"] = json.loads(d["age_groups"])
        d["cell_keys"] = [tuple(k) for k in json.loads(d["cell_keys"])]
        out.append(d)
    return out


def insert_publication(
    districts: list[str],
    age_groups: list[str],
    cell_keys: list[tuple[str, str]],
    total: int,
) -> dict:
    """单事务插入一条公开记录；失败回滚，不会留下半次发布。"""
    created = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with closing(connect()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO publications (districts, age_groups, cell_keys, total, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                json.dumps(list(districts), ensure_ascii=False),
                json.dumps(list(age_groups), ensure_ascii=False),
                json.dumps([list(k) for k in cell_keys], ensure_ascii=False),
                int(total),
                created,
            ),
        )
        pid = cur.lastrowid
    return {
        "id": pid,
        "districts": list(districts),
        "age_groups": list(age_groups),
        "cell_keys": list(cell_keys),
        "total": int(total),
        "created_at": created,
    }
