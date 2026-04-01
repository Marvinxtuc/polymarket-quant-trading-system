from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class BlockBeatsQueryScriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = Path(__file__).resolve().parents[1]
        self.script = self.repo / "scripts" / "blockbeats_query.sh"
        self.fetch_helper = self.repo / "scripts" / "blockbeats_http_fetch.py"

    def _prepare_temp_repo(self) -> tuple[Path, Path]:
        temp_root = Path(tempfile.mkdtemp())
        temp_repo = temp_root / "repo"
        (temp_repo / "scripts").mkdir(parents=True)
        temp_script = temp_repo / "scripts" / "blockbeats_query.sh"
        temp_helper = temp_repo / "scripts" / "blockbeats_http_fetch.py"
        shutil.copy2(self.script, temp_script)
        shutil.copy2(self.fetch_helper, temp_helper)
        temp_script.chmod(0o755)
        temp_helper.chmod(0o755)
        return temp_root, temp_repo

    def test_reads_api_key_and_base_url_from_dotenv(self) -> None:
        temp_root, temp_repo = self._prepare_temp_repo()
        self.addCleanup(shutil.rmtree, temp_root, ignore_errors=True)

        (temp_repo / ".env").write_text(
            "\n".join(
                [
                    "BLOCKBEATS_API_KEY=bb-from-dotenv",
                    "BLOCKBEATS_BASE_URL=https://example.blockbeats.local/v1",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        args_log = temp_root / "python-fetch-args.txt"
        fake_helper = temp_repo / "scripts" / "blockbeats_http_fetch.py"
        fake_helper.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import json",
                    "import pathlib",
                    "import sys",
                    "args = sys.argv[1:]",
                    f"pathlib.Path({str(args_log)!r}).write_text('\\n'.join(args), encoding='utf-8')",
                    "output = args[args.index('--output') + 1]",
                    "pathlib.Path(output).write_text(json.dumps({'ok': True}), encoding='utf-8')",
                ]
            ),
            encoding="utf-8",
        )
        fake_helper.chmod(0o755)

        env = os.environ.copy()
        env.pop("BLOCKBEATS_API_KEY", None)
        env.pop("BLOCKBEATS_BASE_URL", None)
        env["BLOCKBEATS_PYTHON_BIN"] = sys.executable

        proc = subprocess.run(
            [str(temp_repo / "scripts" / "blockbeats_query.sh"), "prediction", "1", "2", "en"],
            cwd=temp_repo,
            env=env,
            text=True,
            capture_output=True,
        )

        self.assertEqual(proc.returncode, 0, proc.stderr)
        args = args_log.read_text(encoding="utf-8")
        self.assertIn("--api-key", args)
        self.assertIn("bb-from-dotenv", args)
        self.assertIn("--url", args)
        self.assertIn("https://example.blockbeats.local/v1/newsflash/prediction?page=1&size=2&lang=en", args)
        self.assertIn('"ok": true', proc.stdout.lower())

    def test_exits_with_clear_message_when_key_is_missing(self) -> None:
        temp_root, temp_repo = self._prepare_temp_repo()
        self.addCleanup(shutil.rmtree, temp_root, ignore_errors=True)

        env = os.environ.copy()
        env.pop("BLOCKBEATS_API_KEY", None)
        env["BLOCKBEATS_ALLOW_PUBLIC_FALLBACK"] = "0"

        proc = subprocess.run(
            [str(temp_repo / "scripts" / "blockbeats_query.sh"), "search", "bitcoin"],
            cwd=temp_repo,
            env=env,
            text=True,
            capture_output=True,
        )

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("BLOCKBEATS_API_KEY is required", proc.stderr)
        self.assertIn(str(temp_repo / ".env"), proc.stderr)

    def test_prediction_falls_back_to_public_feed_without_json_decode_noise(self) -> None:
        temp_root, temp_repo = self._prepare_temp_repo()
        self.addCleanup(shutil.rmtree, temp_root, ignore_errors=True)

        (temp_repo / ".env").write_text(
            "\n".join(
                [
                    "BLOCKBEATS_API_KEY=bb-from-dotenv",
                    "BLOCKBEATS_BASE_URL=https://api-pro.theblockbeats.info/v1",
                    "BLOCKBEATS_PUBLIC_BASE_URL=https://api.theblockbeats.news/v1/open-api",
                    "BLOCKBEATS_DOH_URL=https://1.1.1.1/dns-query",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
        fake_curl = fake_bin / "curl"
        curl_log = temp_root / "curl-log.txt"
        fake_helper = temp_repo / "scripts" / "blockbeats_http_fetch.py"
        fake_curl.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import json",
                    "import pathlib",
                    "import sys",
                    "args = sys.argv[1:]",
                    f"pathlib.Path({str(curl_log)!r}).write_text('\\n'.join(args), encoding='utf-8')",
                    "url = args[-1] if args else ''",
                    "if 'api-pro.theblockbeats.info' in url:",
                    "    sys.stderr.write('curl: (6) Could not resolve host: api-pro.theblockbeats.info\\n')",
                    "    raise SystemExit(6)",
                    "if 'api.theblockbeats.news' in url:",
                    "    print(json.dumps({'status': 0, 'data': {'data': [{'title': 'Fallback headline'}]}}))",
                    "    raise SystemExit(0)",
                    "raise SystemExit(1)",
                ]
            ),
            encoding="utf-8",
        )
        fake_curl.chmod(0o755)
        fake_helper.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import sys",
                    "sys.stderr.write('python fetch failed for pro transport\\n')",
                    "raise SystemExit(1)",
                ]
            ),
            encoding="utf-8",
        )
        fake_helper.chmod(0o755)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BLOCKBEATS_PYTHON_BIN"] = sys.executable

        proc = subprocess.run(
            [str(temp_repo / "scripts" / "blockbeats_query.sh"), "prediction", "1", "1", "en"],
            cwd=temp_repo,
            env=env,
            text=True,
            capture_output=True,
        )

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("falling back to public flash feed", proc.stderr)
        self.assertIn("python fetch failed for pro transport", proc.stderr)
        self.assertNotIn("Expecting value", proc.stderr)
        self.assertIn("Fallback headline", proc.stdout)


if __name__ == "__main__":
    unittest.main()
