from __future__ import annotations

import importlib.util
import json
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


class ReleaseGateAggregateTests(unittest.TestCase):
    def test_release_gate_aggregates_all_required_blocks_from_single_source(self):
        module = _load_release_gate_module()
        repo_root = Path(__file__).resolve().parents[1]
        config_path = repo_root / "scripts" / "gates" / "release_blocks.json"
        release_cfg = module.load_release_blocks(config_path)
        required_blocks = list(release_cfg["required_blocks"])
        self.assertGreaterEqual(len(required_blocks), 9)

        with tempfile.TemporaryDirectory(prefix="release-gate-aggregate-") as temp_dir:
            root = Path(temp_dir)
            for block_id in required_blocks:
                block_dir = root / "reports" / "blocking" / block_id
                block_dir.mkdir(parents=True, exist_ok=True)
                (block_dir / "validation.txt").write_text(
                    f"{block_id} validation PASS command",
                    encoding="utf-8",
                )
                (block_dir / "regression.txt").write_text(
                    f"{block_id} regression PASS gate_tests",
                    encoding="utf-8",
                )
                (block_dir / "self_check.md").write_text(
                    f"# {block_id} Self Check\nscope anti",
                    encoding="utf-8",
                )

            def _runner(_root: Path, block_id: str):
                return (
                    0,
                    f"GATE_BLOCK_RESULT block_id={block_id} static=0 tests=0 behavior=0 docs=0 overall=0\n",
                )

            json_out = root / "reports" / "release" / "go_no_go_summary.json"
            md_out = root / "reports" / "release" / "go_no_go_summary.md"
            code, summary = module.run_release_readiness(
                root_dir=root,
                config_path=config_path,
                json_out=json_out,
                md_out=md_out,
                release_gate_command="bash scripts/gates/gate_release_readiness.sh",
                gate_runner=_runner,
            )

            self.assertEqual(code, 0)
            self.assertEqual(summary["go_no_go"], "GO")
            self.assertEqual(summary["required_blocks"], required_blocks)
            self.assertEqual(summary["required_total"], len(required_blocks))
            self.assertEqual(summary["required_passed"], len(required_blocks))
            self.assertEqual(summary["required_failed"], 0)

            stored = json.loads(json_out.read_text(encoding="utf-8"))
            self.assertEqual(stored["required_blocks"], required_blocks)
            self.assertEqual(stored["go_no_go"], "GO")


if __name__ == "__main__":
    unittest.main()
