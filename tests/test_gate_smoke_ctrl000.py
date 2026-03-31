import unittest
from pathlib import Path


class GateSmokeCtrl000(unittest.TestCase):
    def test_required_files_exist(self):
        required = [
            "prompts/codex_master_prompt.md",
            "prompts/task_template.md",
            "prompts/retry_template.md",
            "docs/blocking/tasks/README.md",
            "scripts/gates/gate_static.sh",
            "scripts/gates/gate_tests.sh",
            "scripts/gates/gate_docs.sh",
            "scripts/gates/gate_block_item.sh",
        ]
        missing = [p for p in required if not Path(p).exists()]
        self.assertFalse(missing, f"missing required files: {missing}")

    def test_reports_placeholders_exist(self):
        base = Path("reports/blocking/CTRL-000")
        self.assertTrue((base / "validation.txt").exists())
        self.assertTrue((base / "self_check.md").exists())


if __name__ == "__main__":
    unittest.main()
