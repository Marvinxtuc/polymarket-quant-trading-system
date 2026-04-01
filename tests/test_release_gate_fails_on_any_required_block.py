from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


def _load_release_gate_module():
    root = Path(__file__).resolve().parents[1]
    module_path = root / "scripts" / "verify_release_readiness.py"
    spec = importlib.util.spec_from_file_location("verify_release_readiness", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load verify_release_readiness.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ReleaseGateFailClosedTests(unittest.TestCase):
    def _seed_reports(self, root: Path, blocks: list[str]) -> None:
        for block_id in blocks:
            block_dir = root / "reports" / "blocking" / block_id
            block_dir.mkdir(parents=True, exist_ok=True)
            (block_dir / "validation.txt").write_text(f"{block_id} validation PASS command", encoding="utf-8")
            (block_dir / "regression.txt").write_text(f"{block_id} regression PASS gate_tests", encoding="utf-8")
            (block_dir / "self_check.md").write_text(f"# {block_id} Self Check\nscope anti", encoding="utf-8")

    def test_release_gate_is_no_go_when_any_required_block_fails(self):
        module = _load_release_gate_module()
        repo_root = Path(__file__).resolve().parents[1]
        config_path = repo_root / "scripts" / "gates" / "release_blocks.json"
        required_blocks = list(module.load_release_blocks(config_path)["required_blocks"])

        with tempfile.TemporaryDirectory(prefix="release-gate-fail-one-") as temp_dir:
            root = Path(temp_dir)
            self._seed_reports(root, required_blocks)

            failed_block = required_blocks[2]

            def _runner(_root: Path, block_id: str):
                if block_id == failed_block:
                    return (
                        1,
                        f"GATE_BLOCK_RESULT block_id={block_id} static=0 tests=1 behavior=0 docs=0 overall=1\n",
                    )
                return (
                    0,
                    f"GATE_BLOCK_RESULT block_id={block_id} static=0 tests=0 behavior=0 docs=0 overall=0\n",
                )

            code, summary = module.run_release_readiness(
                root_dir=root,
                config_path=config_path,
                json_out=root / "reports" / "release" / "go_no_go_summary.json",
                md_out=root / "reports" / "release" / "go_no_go_summary.md",
                release_gate_command="bash scripts/gates/gate_release_readiness.sh",
                gate_runner=_runner,
            )

            self.assertNotEqual(code, 0)
            self.assertEqual(summary["go_no_go"], "NO-GO")
            self.assertGreaterEqual(summary["required_failed"], 1)

    def test_release_gate_is_no_go_when_required_report_is_missing(self):
        module = _load_release_gate_module()
        repo_root = Path(__file__).resolve().parents[1]
        config_path = repo_root / "scripts" / "gates" / "release_blocks.json"
        required_blocks = list(module.load_release_blocks(config_path)["required_blocks"])

        with tempfile.TemporaryDirectory(prefix="release-gate-fail-missing-report-") as temp_dir:
            root = Path(temp_dir)
            self._seed_reports(root, required_blocks)
            # Remove one required report file; gate must fail-closed.
            target_block = required_blocks[0]
            (root / "reports" / "blocking" / target_block / "self_check.md").unlink()

            def _runner(_root: Path, block_id: str):
                return (
                    0,
                    f"GATE_BLOCK_RESULT block_id={block_id} static=0 tests=0 behavior=0 docs=0 overall=0\n",
                )

            code, summary = module.run_release_readiness(
                root_dir=root,
                config_path=config_path,
                json_out=root / "reports" / "release" / "go_no_go_summary.json",
                md_out=root / "reports" / "release" / "go_no_go_summary.md",
                release_gate_command="bash scripts/gates/gate_release_readiness.sh",
                gate_runner=_runner,
            )

            self.assertNotEqual(code, 0)
            self.assertEqual(summary["go_no_go"], "NO-GO")
            failed_rows = [row for row in summary["blocks"] if not row["passed"]]
            self.assertTrue(any("report_structure_invalid" in row["failure_reasons"] for row in failed_rows))


if __name__ == "__main__":
    unittest.main()
