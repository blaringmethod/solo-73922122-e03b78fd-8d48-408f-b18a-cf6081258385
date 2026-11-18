"""关键功能自测：以 TestClient / ASGI 异步客户端驱动应用，使用独立临时数据库。

运行：python tests/selftest.py
覆盖：
- 导入校验、导入后不可更换、预览、安全/不安全发布、重核拒绝；
- 晚到格值追加：仅新组合、旧格不可改、≤16 格、失败不留半批、批次号；
- 发布快照语义：新格落入旧勾选范围也不计入旧总数，审查按当前完整格集合联合进行；
- 旧版库前向迁移（回填 cell_keys 快照，不改写旧数值）；
- 追加与确认发布交错时的锁一致性；
- 公开页不泄露格内检出数/快照；落盘持久化；求解器并发安全。
"""

import asyncio
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

_TMP = Path(tempfile.mkdtemp(prefix="cdc_guard_test_"))
os.environ["DB_PATH"] = str(_TMP / "test.db")  # 须在导入 app 前设置

import httpx  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import app.database as dbmod  # noqa: E402
import app.main as mainmod  # noqa: E402
from app.main import app  # noqa: E402
from app.solver import evaluate  # noqa: E402

SAMPLE = (
    "分区,年龄组,人数,检出数\n"
    "城东,青年,10,2\n"
    "城东,老年,10,8\n"
    "城西,青年,10,8\n"
    "城西,老年,10,8\n"
)

# 稀疏格集合：用于演示"新格落入旧记录勾选范围但不计入旧总数"
# 初始 3 格中城西占两格（旧记录可覆盖多格而不钉死单格），城东仅有青年
SPARSE = (
    "分区,年龄组,人数,检出数\n"
    "城东,青年,10,2\n"
    "城西,青年,10,8\n"
    "城西,老年,10,9\n"
)
# 城东老年晚到：落在后续「城东 × 青年/老年」勾选范围内，但任何旧记录发布时它都不存在
APPEND_MISSING_CELL = "分区,年龄组,人数,检出数\n城东,老年,10,3\n"

results = []


def check(name, cond, extra=""):
    results.append(bool(cond))
    print(f"[{'PASS' if cond else 'FAIL'}] {name}")
    if not cond and extra:
        print("       " + extra.replace("\n", "\n       ")[:800])


def use_db(name: str) -> Path:
    p = _TMP / name
    dbmod.DB_PATH = str(p)  # 所有连接按操作创建并在调用时读取该路径
    return p


def run():
    # ============ 库 A：空库上禁止追加 ============
    use_db("empty.db")
    with TestClient(app) as client:
        r = client.post(
            "/api/append",
            files={"file": ("a.csv", APPEND_MISSING_CELL.encode(), "text/csv")},
        )
        check("未导入时追加被拒绝(400)", r.status_code == 400, r.text)

    # ============ 库 B：稀疏格 + 发布快照语义 ============
    use_db("snapshot.db")
    with TestClient(app) as client:
        def post_csv(path, text, fname="data.csv"):
            return client.post(
                path,
                files={"file": (fname, text.encode("utf-8"), "text/csv")},
            )

        r = post_csv("/api/import", SPARSE)
        check("稀疏格导入成功（3 格）", r.status_code == 201 and r.json()["imported"] == 3, r.text)

        # 发布「城西 × 青年/老年」：当时涵盖 城西/青年(8) + 城西/老年(9)
        r = client.post("/api/publish",
                        json={"districts": ["城西"], "age_groups": ["青年", "老年"]})
        check("发布「城西」成功（快照含 2 格，总数=17）",
              r.status_code == 201 and r.json()["total"] == 17
              and sorted(map(tuple, r.json()["cell_keys"])) == sorted(
                  [("城西", "青年"), ("城西", "老年")]),
              r.text)

        # 追加 城东/老年：与旧记录勾选范围（城西）无交集，且属新组合
        r = post_csv("/api/append", APPEND_MISSING_CELL)
        j = r.json()
        check("晚到格 城东/老年 追加成功（批次1，总格4）",
              r.status_code == 201 and j["appended"] == 1
              and j["batch"] == 1 and j["total_cells"] == 4, r.text)

        st = client.get("/api/admin/state").json()
        check("内部状态含 batch 标记（旧格0/新格1）",
              {c["batch"] for c in st["cells"]} == {0, 1}, str(st["cells"]))

        # 旧记录不得被改写：总数仍为 17，快照仍只有城西 2 格
        pubs = client.get("/api/admin/state").json()["publications"]
        check("旧记录总数与格快照不被追加改写",
              pubs[0]["total"] == 17
              and sorted(map(tuple, pubs[0]["cell_keys"])) == sorted(
                  [("城西", "青年"), ("城西", "老年")]),
              str(pubs[0]))

        # 预览「城东 × 青年/老年」：当前交集为 2 格，候选总数=2+3=5
        r = client.post("/api/preview",
                        json={"districts": ["城东"], "age_groups": ["青年", "老年"]})
        j = r.json()
        check("预览按当前格集合计算（总数=5，2 格）",
              r.status_code == 200 and j["total"] == 5 and j["cells_matched"] == 2, r.text)

        # 联合审查（旧记录快照 + 候选，作用于当前完整 4 格集合）：
        # 旧记录只约束城西两格=17；城东两格仅受候选 x城东青年+x城东老年=5 与界限约束，
        # 新小样本格 城东/老年(det=3) 仍可取 0（另一格=5≤10）与 ≥5（另一格=0）→ 不受影响。
        check("联合审查通过（新小样本格未被旧总数钉死）", j["safe"], r.text)
        aff_new = j.get("affected", [])
        check("新小样本格 城东/老年 仍保留取0与≥5可行解（不在受影响列表）",
              not any(a["district"] == "城东" and a["age_group"] == "老年" for a in aff_new),
              str(aff_new))

        # 反证：假设系统错误地把新格计入旧记录——把旧记录解释为「老年」两格之和
        # （城西老年9 + 城东老年3 = 12，即旧总数 12 被错误地含新格）。再叠加候选
        # 城东两格=5，则新格 城东/老年(det=3) 下界被钉到 2，无法取 0 → 必判受影响。
        # 正确快照口径（旧约束不含新格）不会产生该误判，上面的预览已验证 safe=True。
        cells = dbmod.get_cells()
        bad_pubs = [{"districts": ["城西", "城东"], "age_groups": ["老年"],
                     "cell_keys": [("城西", "老年"), ("城东", "老年")], "total": 12}]
        wrong = evaluate(
            cells, bad_pubs,
            {"districts": ["城东"], "age_groups": ["青年", "老年"], "total": 5},
        )
        hit = next((a for a in wrong["affected"]
                    if a["district"] == "城东" and a["age_group"] == "老年"), None)
        check("反证：错误口径（新格计入旧总数）使新格不可取0而被误判",
              (not wrong["safe"]) and hit is not None
              and hit["min"] == 2 and hit["zero_feasible"] is False,
              str(wrong))

        # 端到端再做一种可拒绝场景：发布仅含新格的单格窗口「城东 × 老年」，
        # 候选只含 城东/老年 一格 → 其变量被总数 3 钉死，联合审查正确拒绝(409)。
        r = client.post("/api/publish",
                        json={"districts": ["城东"], "age_groups": ["老年"]})
        check("单格晚到窗口发布被联合审查拒绝(409)，受影响格为 城东/老年",
              r.status_code == 409
              and any(a["district"] == "城东" and a["age_group"] == "老年"
                      for a in r.json()["detail"]["affected"]), r.text)
        check("拒绝后未新增公开记录，旧记录保持不变",
              len(dbmod.get_publications()) == 1
              and dbmod.get_publications()[0]["total"] == 17)

        # 追加校验：冲突 / 上限 / 半批
        r = post_csv("/api/append", "分区,年龄组,人数,检出数\n城东,青年,10,9\n")
        check("追加已存在组合被拒绝(409)，旧格不可修改", r.status_code == 409, r.text)
        old_cell = [c for c in dbmod.get_cells() if (c["district"], c["age_group"]) == ("城东", "青年")][0]
        check("被拒后旧格值保持不变（仍为 2）", old_cell["detected"] == 2, str(old_cell))

        mixed = (
            "分区,年龄组,人数,检出数\n"
            "城南,青年,10,1\n"          # 新格
            "城东,老年,10,9\n"          # 与现有格冲突
        )
        r = post_csv("/api/append", mixed)
        check("一批中含冲突组合整批拒绝(409)", r.status_code == 409, r.text)
        check("整批失败不留半批（城南/青年 未写入）",
              not any((c["district"], c["age_group"]) == ("城南", "青年") for c in dbmod.get_cells()))

        over = "分区,年龄组,人数,检出数\n" + "\n".join(
            f"区{i},组{i},10,1" for i in range(13)
        )  # 现有 4 格 + 13 = 17 > 16
        r = post_csv("/api/append", over)
        check("追加后超过 16 格整批拒绝(400)", r.status_code == 400, r.text)
        check("超限拒绝后格数不变（仍 4 格）", len(dbmod.get_cells()) == 4)

        # 恰好补到 16 格应成功
        fill = "分区,年龄组,人数,检出数\n" + "\n".join(
            f"补{i},格{i},10,1" for i in range(12)
        )
        r = post_csv("/api/append", fill)
        check("补足到 16 格成功", r.status_code == 201 and r.json()["total_cells"] == 16, r.text)
        r = post_csv("/api/append", "分区,年龄组,人数,检出数\n再多一区,某组,10,1\n")
        check("已满 16 格后继续追加被拒绝(400)", r.status_code == 400, r.text)

        # 追加文件内自身重复组合 → 400
        r = post_csv("/api/append",
                     "分区,年龄组,人数,检出数\n新X,组1,10,1\n新X,组1,10,2\n")
        check("追加文件内组合重复被拒绝(400)", r.status_code == 400, r.text)

        # 公开接口不泄露格内检出数与快照
        pubj = client.get("/api/public").json()
        check("公开 API 不含 detected/cell_keys/batch",
              all("detected" not in c and "batch" not in c for c in pubj["cells"])
              and all("cell_keys" not in p for p in pubj["publications"]), str(pubj)[:400])
        check("公开汇总字段集合固定",
              all(set(p) == {"id", "districts", "age_groups", "total", "created_at"}
                  for p in pubj["publications"]))
        check("公开页可访问且无格值字样接口数据", client.get("/public").status_code == 200)

    # ============ 库 B 持久化（等价重启） ============
    conn = sqlite3.connect(dbmod.DB_PATH)
    n_cell = conn.execute("SELECT COUNT(*) FROM cells").fetchone()[0]
    n_pub = conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0]
    cols = {r[1] for r in conn.execute("PRAGMA table_info(publications)").fetchall()}
    conn.close()
    check("格值与公开记录落盘（16 格 / 1 记录）", n_cell == 16 and n_pub == 1)
    check("publications 含 cell_keys 快照列", "cell_keys" in cols)

    # ============ 库 C：旧版库前向迁移 ============
    legacy = _TMP / "legacy.db"
    conn = sqlite3.connect(legacy)
    conn.executescript(
        """
        CREATE TABLE cells (
            district TEXT NOT NULL, age_group TEXT NOT NULL,
            population INTEGER NOT NULL, detected INTEGER NOT NULL,
            PRIMARY KEY (district, age_group));
        CREATE TABLE publications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            districts TEXT NOT NULL, age_groups TEXT NOT NULL,
            total INTEGER NOT NULL, created_at TEXT NOT NULL);
        INSERT INTO cells VALUES ('城东','青年',10,2),('城东','老年',10,8);
        INSERT INTO publications (districts, age_groups, total, created_at)
            VALUES ('["城东"]','["青年","老年"]',10,'2026-01-01T00:00:00+00:00');
        """
    )
    conn.commit()
    conn.close()
    dbmod.DB_PATH = str(legacy)
    dbmod.init_db()  # 应用启动时执行的迁移
    cells = dbmod.get_cells()
    pubs = dbmod.get_publications()
    check("迁移后旧格 batch 回填为 0", all(c["batch"] == 0 for c in cells))
    check("迁移后旧记录 cell_keys 按当时交集回填（2 格）且 total 不变",
              pubs[0]["total"] == 10
              and set(pubs[0]["cell_keys"]) == {("城东", "青年"), ("城东", "老年")},
              str(pubs[0]))
    # 迁移后再追加新格，旧记录快照仍冻结
    with TestClient(app) as client:
        r = client.post(
            "/api/append",
            files={"file": ("a.csv",
                            "分区,年龄组,人数,检出数\n城西,青年,10,1\n".encode(),
                            "text/csv")},
        )
        check("迁移库上可继续追加", r.status_code == 201, r.text)
        pubs = dbmod.get_publications()
        check("迁移库追加后旧记录快照仍冻结（不含新格）",
              set(pubs[0]["cell_keys"]) == {("城东", "青年"), ("城东", "老年")}
              and pubs[0]["total"] == 10)

    # ============ 库 D：追加与确认发布交错（锁一致性） ============
    use_db("interleave.db")
    with TestClient(app) as client:
        r = client.post(
            "/api/import",
            files={"file": ("d.csv", SPARSE.encode(), "text/csv")},
        )
        check("交错场景导入成功", r.status_code == 201, r.text)

        # evaluate 在工作线程中经 asyncio.to_thread 调用，故用线程事件打桩
        import threading

        started = threading.Event()
        release = threading.Event()
        orig_eval = mainmod.evaluate

        def gated_eval(cells_arg, pubs_arg, cand_arg):
            started.set()
            if not release.wait(timeout=15):
                raise RuntimeError("测试闸门超时")
            return orig_eval(cells_arg, pubs_arg, cand_arg)

        mainmod.evaluate = gated_eval
        transport = httpx.ASGITransport(app=app)
        try:
            async def wait_started():
                for _ in range(200):
                    if started.is_set():
                        return True
                    await asyncio.sleep(0.05)
                return False

            async def scenario():
                async with httpx.AsyncClient(transport=transport, base_url="http://t") as ac:
                    pub_task = asyncio.create_task(
                        ac.post("/api/publish",
                                json={"districts": ["城西"], "age_groups": ["青年", "老年"]})
                    )
                    if not await wait_started():
                        raise RuntimeError("发布求解未在预期时间内进入闸门")
                    # 求解进行中发起追加：必须在写锁外排队，不能夹进发布快照
                    append_task = asyncio.create_task(
                        ac.post("/api/append",
                                files={"file": ("a.csv", APPEND_MISSING_CELL.encode(),
                                                "text/csv")})
                    )
                    await asyncio.sleep(0.3)
                    check("追加在发布持锁期间排队等待", not append_task.done())
                    release.set()
                    rp = await asyncio.wait_for(pub_task, timeout=15)
                    ra = await asyncio.wait_for(append_task, timeout=15)
                    return rp, ra

            rp, ra = asyncio.run(scenario())
        finally:
            mainmod.evaluate = orig_eval

        check("交错：发布先完成且快照只含发布当时的城西 2 格",
              rp.status_code == 201 and rp.json()["total"] == 17
              and sorted(map(tuple, rp.json()["cell_keys"])) == sorted(
                  [("城西", "青年"), ("城西", "老年")]),
              rp.text)
        check("交错：排队的追加随后成功", ra.status_code == 201
              and ra.json()["appended"] == 1, ra.text)
        pubs = dbmod.get_publications()
        check("交错后旧记录仍未被新格改写",
              pubs[0]["total"] == 17
              and sorted(pubs[0]["cell_keys"]) == sorted(
                  [("城西", "青年"), ("城西", "老年")]))

    # ============ 库 E：原有主流程回归 + 保存失败原子性 + 并发求解 ============
    use_db("main.db")
    with TestClient(app) as client:
        def import_csv(text):
            return client.post(
                "/api/import",
                files={"file": ("data.csv", text.encode("utf-8"), "text/csv")},
            )

        # ---- 1. 导入校验 ----
        r = import_csv("分区,年龄组,人数,检出数\nA,青,4,1\n")
        check("人数<5 被拒绝", r.status_code == 400, r.text)
        r = import_csv("分区,年龄组,人数,检出数\nA,青,10,11\n")
        check("检出数>人数 被拒绝", r.status_code == 400, r.text)
        r = import_csv("分区,年龄组,人数,检出数\nA,青,10,-1\n")
        check("检出数<0 被拒绝", r.status_code == 400, r.text)
        r = import_csv("分区,年龄组,人数,检出数\nA,青,10,1.5\n")
        check("检出数非整数 被拒绝", r.status_code == 400, r.text)
        r = import_csv("分区,年龄组,人数,检出数\nA,青,10,1\nA,青,10,2\n")
        check("组合重复 被拒绝", r.status_code == 400, r.text)
        many = "分区,年龄组,人数,检出数\n" + "\n".join(
            f"区{i},青,10,1" for i in range(17)
        )
        r = import_csv(many)
        check("超过16格 被拒绝", r.status_code == 400, r.text)
        r = import_csv("分区,年龄组,人数\nA,青,10\n")
        check("缺少检出数列 被拒绝", r.status_code == 400, r.text)

        # ---- 2. 正常导入 + 不可更换 ----
        r = import_csv(SAMPLE)
        check("示例导入成功", r.status_code == 201 and r.json()["imported"] == 4, r.text)
        r = import_csv(SAMPLE)
        check("重复导入被拒绝（导入后不可更换）", r.status_code == 409, r.text)

        # ---- 3. 预览 ----
        r = client.post("/api/preview", json={"districts": ["城东"], "age_groups": ["青年", "老年"]})
        j = r.json()
        check("预览「城东」总数=10 且安全",
              r.status_code == 200 and j["total"] == 10 and j["safe"], r.text)

        # ---- 4. 两窗口各自安全、合并不安全 ----
        r = client.post("/api/preview", json={"districts": ["城东", "城西"], "age_groups": ["老年"]})
        j = r.json()
        check("窗口「老年」单独预览安全（总数=16）",
              r.status_code == 200 and j["total"] == 16 and j["safe"], r.text)

        r = client.post("/api/publish", json={"districts": ["城东"], "age_groups": ["青年", "老年"]})
        check("发布「城东」成功（总数=10）",
              r.status_code == 201 and r.json()["total"] == 10, r.text)

        r = client.post("/api/preview", json={"districts": ["城东", "城西"], "age_groups": ["老年"]})
        j = r.json()
        aff = j.get("affected", [])
        check("合并后预览「老年」不安全", r.status_code == 200 and not j["safe"], r.text)
        check("受影响格为 城东/青年，可行范围[0,4]，不可取≥5",
              len(aff) == 1
              and aff[0]["district"] == "城东" and aff[0]["age_group"] == "青年"
              and aff[0]["min"] == 0 and aff[0]["max"] == 4
              and aff[0]["zero_feasible"] and not aff[0]["ge5_feasible"], r.text)

        r = client.post("/api/publish", json={"districts": ["城东", "城西"], "age_groups": ["老年"]})
        check("确认发布「老年」被按最新公开记录重核拒绝(409)", r.status_code == 409, r.text)
        d = r.json().get("detail", {})
        check("409 响应含受影响格与可行范围",
              isinstance(d, dict) and d.get("affected")
              and d["affected"][0]["max"] == 4, r.text)

        # ---- 5. 与已公开记录合并仍安全的发布可继续 ----
        r = client.post("/api/publish", json={"districts": ["城东", "城西"], "age_groups": ["青年"]})
        check("发布「青年」（与「城东」合并仍安全）成功",
              r.status_code == 201 and r.json()["total"] == 10, r.text)
        r = client.post("/api/publish", json={"districts": ["城东", "城西"], "age_groups": ["老年"]})
        check("「老年」仍被拒绝", r.status_code == 409, r.text)

        # ---- 6. 公开页不泄露格内检出数 ----
        r = client.get("/api/public")
        j = r.json()
        check("公开 API 不含 detected 字段",
              r.status_code == 200 and all("detected" not in c for c in j["cells"]), r.text)
        check("公开 API 含人数与 2 条获准汇总",
              len(j["cells"]) == 4 and all("population" in c for c in j["cells"])
              and len(j["publications"]) == 2, r.text)
        check("公开汇总不含格值字段",
              all(set(p) == {"id", "districts", "age_groups", "total", "created_at"}
                  for p in j["publications"]), r.text)
        r = client.get("/public")
        check("公开页可访问", r.status_code == 200 and "公开" in r.text)
        r = client.get("/")
        check("内部审查台可访问", r.status_code == 200 and "审查台" in r.text)

        # ---- 7. 持久化（直接读库文件，等价于重启后再读） ----
        conn = sqlite3.connect(dbmod.DB_PATH)
        n_pub = conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0]
        n_cell = conn.execute("SELECT COUNT(*) FROM cells").fetchone()[0]
        conn.close()
        check("公开记录与格值已落盘（重启保留）", n_pub == 2 and n_cell == 4)

        # ---- 8. 其他边界 ----
        r = client.post("/api/preview", json={"districts": ["不存在"], "age_groups": ["青年"]})
        check("未知分区被拒绝", r.status_code == 400, r.text)
        r = client.post("/api/publish", json={"districts": [], "age_groups": ["青年"]})
        check("空选择被拒绝", r.status_code == 400, r.text)
        r = client.post("/api/preview", json={"districts": ["城东"], "age_groups": []})
        check("空年龄组被拒绝", r.status_code == 400, r.text)

        # ---- 9. 保存失败不留半次发布 ----
        before = len(client.get("/api/public").json()["publications"])

        # 9a. 接口层：插入时发生异常 → 500 且无记录产生
        orig_insert = mainmod.insert_publication
        def _boom(*a, **k):
            raise RuntimeError("模拟保存失败")
        mainmod.insert_publication = _boom
        try:
            r = client.post(
                "/api/publish",
                json={"districts": ["城东", "城西"], "age_groups": ["青年"]},
            )
            check("保存时异常返回 500", r.status_code == 500, r.text)
        finally:
            mainmod.insert_publication = orig_insert
        after = len(client.get("/api/public").json()["publications"])
        check("保存失败未留下半次发布", after == before, f"before={before} after={after}")

        # 9b. 存储层：库不可打开时写入抛错，且库内容不变
        orig_path = dbmod.DB_PATH
        dbmod.DB_PATH = str(_TMP)  # 指向目录：SQLite 无法打开
        try:
            dbmod.insert_publication(["x"], ["y"], [("x", "y")], 1)
            check("库不可打开时写入抛错", False, "未抛错")
        except Exception:
            check("库不可打开时写入抛错", True)
        finally:
            dbmod.DB_PATH = orig_path
        after = len(client.get("/api/public").json()["publications"])
        check("库写失败后记录数不变", after == before, f"before={before} after={after}")

        # ---- 10. 求解器并发安全（z3 每评估独立 Context） ----
        from concurrent.futures import ThreadPoolExecutor

        cells = dbmod.get_cells()
        pubs = dbmod.get_publications()
        cand = {"districts": ["城东", "城西"], "age_groups": ["老年"], "total": 16}
        with ThreadPoolExecutor(max_workers=8) as ex:
            outs = list(ex.map(lambda _: evaluate(cells, pubs, cand), range(40)))
        check(
            "40 路并发求解结果一致且正确",
            all(
                (not o["safe"])
                and len(o["affected"]) == 1
                and o["affected"][0]["max"] == 4
                for o in outs
            ),
            str(outs[:2]),
        )


run()
print(f"\n{sum(results)}/{len(results)} 通过")
sys.exit(0 if all(results) else 1)
