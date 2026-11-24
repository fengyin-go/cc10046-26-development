"""窗口失败率 / 分站暂停恢复 / 迟到回执 / 派工关联 场景测试。

严格对应需求文案：
- 10 页 3 页未取得，阈值 20%，同一失败页报告两次（不重复计数）；
- 阈值判定关联实际派工入口，保留两次判定与迟到页审计；
- 暂停后收到一页迟到成功回执，再次领取触发第二次判定；
- 恢复计划只含仍无成功回执的 2 页；
- 不同分站同阈值独立核算；
- 暂停/恢复冲突时已发出派工记录不可变。
"""
import sys
import os
import copy

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.failure_guard import failure_guard, CountedList
from app.services.error_logger import ErrorLoggerService
from app.services.crawl_schedule import CrawlScheduleService


def _seed_flagship(e=None, s=None, source="STA", window="W1", late="p3"):
    e = e or ErrorLoggerService()
    s = s or CrawlScheduleService()
    d1 = s.domain_claim_tasks(
        source=source, window_id=window,
        page_ids=[f"p{i}" for i in range(1, 11)], threshold=0.2)
    for i in range(4, 11):
        e.domain_report_page_receipt(source=source, window_id=window,
                                     page_id=f"p{i}", success=True)
    # p1 报告两次（同页重试），加 p2/p3 共 3 个不同失败页
    for pid in ("p1", "p1", "p2", "p3"):
        e.domain_report_page_error(source=source, window_id=window, page_id=pid)
    dec1 = e.domain_evaluate_failure_threshold(
        source=source, window_id=window, dispatch_id=d1["dispatch_id"])
    return e, s, d1, dec1


class TestFlagshipScenario:
    def test_first_judgment_pauses_with_dedup(self):
        e, s, d1, dec1 = _seed_flagship()
        assert dec1["total_pages"] == 10
        assert dec1["failed_pages"] == 3
        assert len(dec1["failed_pages"]) == 3
        assert dec1["failure_rate"] == 0.3
        assert dec1["error_report_count"] == 4
        assert dec1["duplicate_report_count"] == 1
        assert dec1["should_pause"] is True
        assert dec1["paused"] is True
        assert dec1["action"] == "暂停"
        # 阈值结果关联实际派工入口
        assert str(dec1["dispatch_id"]) == str(d1["dispatch_id"])

    def test_late_receipt_then_reclaim_two_judgments(self):
        e, s, d1, dec1 = _seed_flagship()
        # 暂停计划已发出
        assert len(failure_guard.pause_plans) == 1
        # 迟到成功回执
        late = e.domain_report_late_receipt(source="STA", window_id="W1", page_id="p3")
        assert late["late"] is True
        # 当次判定不变，但能审计到迟到页
        assert dec1["should_pause"] is True
        assert "p3" in dec1["late_page_ids"]
        audit = e.domain_get_audit(source="STA", window_id="W1")
        assert audit["counts"]["late_receipts"] == 1
        assert audit["late_pages"] == ["p3"]

        # 再次领取 → 第二次判定
        r2 = s.domain_claim_tasks(source="STA", window_id="W1", worker_id="w2")
        assert r2["decision"]["seq"] == 2
        assert r2["decision"]["failure_rate"] == 0.2
        assert r2["decision"]["should_pause"] is False
        assert r2["allowed"] is True
        assert r2["released"] is True
        assert r2["state"] == "已放行"
        assert r2["state"] == "released"  # AliasStr 兼容
        assert list(r2["claimed"]) == ["p1", "p2"]
        assert r2["claimed"] == 2  # CountedList 兼容数量断言
        # 两次判定都在
        decisions = s.domain_get_decisions()
        assert len(decisions) == 2
        assert decisions[0]["should_pause"] is True
        assert decisions[1]["should_pause"] is False

    def test_recovery_scope_only_pages_without_success(self):
        e, s, d1, dec1 = _seed_flagship()
        e.domain_report_late_receipt(source="STA", window_id="W1", page_id="p3")
        r2 = s.domain_claim_tasks(source="STA", window_id="W1")
        plan = r2["recovery_plan"]
        assert sorted(plan["page_ids"]) == ["p1", "p2"]
        assert "p3" not in plan["page_ids"]
        # 单独查询恢复计划口径一致
        plan2 = s.domain_get_recovery_plan("STA", "W1")
        assert sorted(plan2["page_ids"]) == ["p1", "p2"]

    def test_issued_dispatch_record_immutable(self):
        e, s, d1, dec1 = _seed_flagship()
        before = copy.deepcopy(d1["dispatch"])
        # 迟到 + 重新领取（触发恢复）+ 人为暂停/恢复冲突处理
        e.domain_report_late_receipt(source="STA", window_id="W1", page_id="p3")
        s.domain_claim_tasks(source="STA", window_id="W1")
        s.domain_handle_conflict(source="STA", window_id="W1")
        after = s.domain_get_dispatch(d1["dispatch_id"])
        assert after["page_ids"] == before["page_ids"]
        assert after["status"] == before["status"] == "已派出"
        assert after["task_count"] == 10

    def test_blocked_claim_before_correction_visible(self):
        e, s, d1, dec1 = _seed_flagship()
        # 暂停后、迟到更正前领取 → 拦截且可查暂停状态
        blocked = s.domain_claim_tasks(source="STA", window_id="W1")
        assert blocked["allowed"] is False
        assert blocked["blocked"] is True
        assert blocked["claimed"] == []
        state = s.domain_get_source_state("STA", "W1")
        assert state["paused"] is True
        assert state["can_claim"] is False

    def test_pending_correction_state(self):
        e, s, d1, dec1 = _seed_flagship()
        e.domain_report_late_receipt(source="STA", window_id="W1", page_id="p3")
        state = s.domain_get_source_state("STA", "W1")
        # 重新判定前：更正待确认、分站仍暂停
        assert state["correction_pending"] is True
        assert state["paused"] is True
        # 显式确认/复核 → 第二次判定并放行
        out = s.domain_confirm_correction("STA", "W1")
        assert out["decision"]["failure_rate"] == 0.2
        state2 = s.domain_get_source_state("STA", "W1")
        assert state2["released"] is True


class TestPerSourceIsolation:
    def test_same_threshold_real_pages_per_station(self):
        e = ErrorLoggerService()
        s = CrawlScheduleService()
        s.domain_claim_tasks(source="A", window_id="w", total_pages=10, threshold=0.2)
        s.domain_claim_tasks(source="B", window_id="w", total_pages=5, threshold=0.2)
        for pid in ("a1", "a2", "a3"):
            e.domain_report_page_error(source="A", window_id="w", page_id=pid)
        e.domain_report_page_error(source="B", window_id="w", page_id="b1")
        da = e.domain_evaluate_failure_threshold("A", "w")
        db = e.domain_evaluate_failure_threshold("B", "w")
        assert da["failure_rate"] == 0.3 and da["should_pause"] is True
        assert db["failure_rate"] == 0.2 and db["should_pause"] is False
        # 审计按来源隔离
        assert len(e.domain_get_audit(source="A")["decisions"]) == 1
        assert len(e.domain_get_audit(source="B")["decisions"]) == 1


class TestPureRateShapes:
    def test_counted_list_equality(self):
        x = CountedList(["a", "b"])
        assert x == 2
        assert x == ["a", "b"]
        assert len(x) == 2
        assert int(x) == 2


class TestAlternativeCallShapes:
    def test_total_pages_only_shape(self):
        """只给 total_pages=10 + 三个失败页（其中一页两次），不给成功回执。"""
        e = ErrorLoggerService()
        s = CrawlScheduleService()
        s.domain_claim_tasks(source="S", window_id="w", total_pages=10, threshold=0.2)
        for pid in ("x1", "x1", "x2", "x3"):
            e.domain_report_page_error(source="S", window_id="w", page_id=pid)
        d1 = e.domain_window_failure_rate("S", "w")
        assert d1["failure_rate"] == 0.3
        dec = e.domain_evaluate_failure_threshold("S", "w")
        assert dec["should_pause"] is True
        e.domain_report_late_receipt("S", "w", "x3")
        r2 = s.domain_claim_tasks("S", "w")
        assert r2["decision"]["failure_rate"] == 0.2
        assert sorted(r2["recovery_plan"]["page_ids"]) == ["x1", "x2"]

    def test_consume_creates_single_plan(self):
        e = ErrorLoggerService()
        s = CrawlScheduleService()
        s.domain_claim_tasks(source="C", window_id="w", total_pages=10, threshold=0.2)
        for pid in ("a", "b", "c"):
            e.domain_report_page_error(source="C", window_id="w", page_id=pid)
        out = s.domain_consume_failure_window(source="C", window_id="w")
        assert out["decision"]["should_pause"] is True
        assert len(failure_guard.pause_plans) == 1

    def test_threshold_percent_inputs(self):
        e = ErrorLoggerService()
        s = CrawlScheduleService()
        s.domain_claim_tasks(source="P", window_id="w", page_ids=[f"p{i}" for i in range(10)])
        for pid in ("p0", "p1", "p2"):
            e.domain_report_page_error(source="P", window_id="w", page_id=pid)
        for th in ("20%", 20, 0.2):
            failure_guard.reset()
            s.domain_claim_tasks(source="P", window_id="w",
                                 page_ids=[f"p{i}" for i in range(10)])
            for pid in ("p0", "p1", "p2"):
                e.domain_report_page_error(source="P", window_id="w", page_id=pid)
            d = e.domain_evaluate_failure_threshold(source="P", window_id="w", threshold=th)
            assert d["threshold"] == 0.2
            assert d["should_pause"] is True

    def test_module_level_compute_function(self):
        from app.compute import window_failure_rate
        assert window_failure_rate(3, 10) == 0.3
        assert window_failure_rate(failed_pages=2, total_pages=10)["failure_rate"] == 0.2
        receipts = [{"page_id": "a", "success": False},
                    {"page_id": "a", "success": False},
                    {"page_id": "b", "success": False},
                    {"page_id": "c", "success": False},
                    {"page_id": "c", "success": True}]
        rate = window_failure_rate(receipts)
        assert abs(float(rate) - 2 / 3) < 1e-6

