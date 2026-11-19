"""Unit checks for the verifier and local process supervision, not a queue run."""
import argparse
import contextlib
import copy
from io import StringIO
import os
from pathlib import Path
import shlex
import signal
import sys
import tempfile
import unittest

from chaos import Run


class HarnessTest(unittest.TestCase):
    def setUp(self):
        td = self.enterContext(tempfile.TemporaryDirectory())
        self.enterContext(contextlib.redirect_stdout(StringIO()))
        self.run = Run(argparse.Namespace(
            output=td, project=None, mode="local", work_ms=100,
            binary=str(Path(td) / "worker"),
        ))
        self.addCleanup(self.run.log_file.close)

    def snapshot(self):
        def row(i, job_id, status, attempt):
            return dict(
                key=self.run.prefix + str(i), id=job_id, status=status,
                attempt=attempt, leased=False, input=f"training-{i}",
                fail=i % 97 == 0, effects=int(status == "succeeded"), bad_effects=0,
            )
        return dict(orphan_effects=0, jobs=[
            row(1, "42", "succeeded", 2), row(5, "43", "cancelled", 0),
            row(97, "44", "failed", 1),
        ])

    def test_invariants_detect_broken_outcomes(self):
        self.run.kills = 1
        ids, cancels = {1: "42", 5: "43", 97: "44"}, {5}
        snapshot = self.snapshot()
        self.run.verify(snapshot, ids, cancels)
        for fields in [
            {"id": "100"}, {"leased": True}, {"input": "corrupted"}, {"fail": True},
            {"effects": 0}, {"effects": 2}, {"bad_effects": 1},
            {"status": "running"}, {"status": "cancelled"}, {"attempt": 1},
        ]:
            with self.subTest(fields=fields), self.assertRaises(RuntimeError):
                wrong = copy.deepcopy(snapshot)
                wrong["jobs"][0].update(fields)
                self.run.verify(wrong, ids, cancels)
        for jobs in [snapshot["jobs"][1:], snapshot["jobs"] + snapshot["jobs"][:1]]:
            with self.assertRaises(RuntimeError):
                self.run.verify(dict(snapshot, jobs=jobs), ids, cancels)
        with self.assertRaises(RuntimeError):
            self.run.verify(dict(snapshot, orphan_effects=1), ids, cancels)

    @unittest.skipUnless(os.name == "posix", "requires Unix process signals")
    def test_kill_and_cleanup_premature_exit(self):
        fixture = Path(self.run.args.binary)
        fixture.write_text(f"#!/bin/sh\nexec {shlex.quote(sys.executable)} -c 'import time; time.sleep(30)'\n")
        fixture.chmod(0o755)
        self.run.start_worker(0)
        self.assertEqual(self.run.stop_worker(0, kill=True), -signal.SIGKILL)
        fixture.write_text("#!/bin/sh\nexit 17\n")
        self.run.start_worker(0)
        child, path = self.run.workers[0]
        self.assertEqual(child.wait(timeout=5), 17)
        with self.assertRaisesRegex(RuntimeError, "exited before planned termination"):
            self.run.stop_worker(0, kill=False)
        self.assertTrue(path.is_file())
        self.assertNotIn(0, self.run.workers)


if __name__ == "__main__":
    unittest.main()
