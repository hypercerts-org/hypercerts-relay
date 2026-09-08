"""Offline fixtures for scoped updates and conflict stops."""
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("jetstream-upstream.py").resolve()


class UpdateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.up = self.root / "upstream"
        self.fork = self.root / "fork"
        for repo in (self.up, self.fork):
            repo.mkdir()
            self.git(repo, "init", "-b", "main")
            self.git(repo, "config", "user.name", "Fixture")
            self.git(repo, "config", "user.email", "fixture@example.invalid")
        (self.up / "runtime.go").write_text("baseline\n")
        (self.up / "excluded.go").write_text("excluded\n")
        self.commit(self.up)
        self.base = self.git(self.up, "rev-parse", "HEAD").strip()
        (self.up / "runtime.go").write_text("upstream change\n")
        (self.up / "excluded.go").write_text("must not enter fork\n")
        self.commit(self.up)
        self.target = self.git(self.up, "rev-parse", "HEAD").strip()
        (self.fork / ".hypercerts").mkdir()
        (self.fork / "jetstream").mkdir()
        (self.fork / ".hypercerts/jetstream-upstream-base").write_text(self.base + "\n")
        (self.fork / ".hypercerts/jetstream-upstream-paths").write_text("runtime.go\n")
        (self.fork / "jetstream/runtime.go").write_text("baseline\n")
        self.commit(self.fork)
        self.git(self.fork, "fetch", str(self.up), "main")

    def git(self, repo, *args):
        return subprocess.check_output(["git", "-C", str(repo), *args], stderr=subprocess.DEVNULL, text=True)

    def commit(self, repo):
        self.git(repo, "add", ".")
        self.git(repo, "commit", "-m", "fixture")

    def run_update(self):
        return subprocess.run(["python3", str(SCRIPT), self.target, "--apply"], cwd=self.fork, capture_output=True, text=True)

    def test_scoped_apply(self):
        self.git(self.fork, "switch", "-c", "review")
        result = self.run_update()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.fork / "jetstream/runtime.go").read_text(), "upstream change\n")
        self.assertFalse((self.fork / "jetstream/excluded.go").exists())
        self.assertEqual((self.fork / ".hypercerts/jetstream-upstream-base").read_text().strip(), self.target)

    def test_main_refused(self):
        self.assertNotEqual(self.run_update().returncode, 0)
        self.assertEqual((self.fork / "jetstream/runtime.go").read_text(), "baseline\n")

    def test_conflict_preserves_baseline(self):
        self.git(self.fork, "switch", "-c", "review")
        (self.fork / "jetstream/runtime.go").write_text("// hypercerts: owned change\n")
        self.commit(self.fork)
        result = self.run_update()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("runtime.go", result.stdout)
        self.assertEqual((self.fork / ".hypercerts/jetstream-upstream-base").read_text().strip(), self.base)
        self.assertTrue(self.git(self.fork, "diff", "--name-only", "--diff-filter=U"))


if __name__ == "__main__":
    unittest.main()
