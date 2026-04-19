"""Unit tests for task_bus_reader.

Run with: python3 -m unittest discover src/tests
"""
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

# Allow import regardless of where tests are run from
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from task_bus_reader import TaskEntry, read_and_summarize, read_task_files, summarize_tasks


def _write_task(tmp: Path, name: str, status: str, title: str = "A title") -> None:
    content = textwrap.dedent(f"""\
        ---
        id: {name.replace('.md', '')}
        status: {status}
        title: {title}
        ---
        body
    """)
    (tmp / name).write_text(content, encoding="utf-8")


class TestReadTaskFilesExcludesTemplate(unittest.TestCase):
    def test_excludes_template(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            tmp = Path(d)
            _write_task(tmp, "2026-04-01-task-a.md", "proposed", "Task A")
            _write_task(tmp, "2026-04-02-task-b.md", "done", "Task B")
            _write_task(tmp, "_template.md", "proposed", "Template placeholder")

            entries = read_task_files(tmp)

        self.assertEqual(len(entries), 2)
        names = [e.file_name for e in entries]
        self.assertNotIn("_template.md", names)


class TestSummarizeShowsOnlyNonzeroCounts(unittest.TestCase):
    def test_nonzero_counts_only(self) -> None:
        entries = [
            TaskEntry(file_name="a.md", status="done", task_id="t1", title="Done task 1"),
            TaskEntry(file_name="b.md", status="done", task_id="t2", title="Done task 2"),
            TaskEntry(file_name="c.md", status="proposed", task_id="t3", title="Proposed task"),
        ]

        result = summarize_tasks(entries)

        self.assertIn("done: 2", result)
        self.assertIn("proposed: 1", result)
        self.assertNotIn("detected", result)
        self.assertNotIn("blocked", result)


class TestSummarizeCapsActiveList(unittest.TestCase):
    def test_caps_at_15_entries(self) -> None:
        entries = [
            TaskEntry(
                file_name=f"t{i}.md",
                status="proposed",
                task_id=f"task-{i:02d}",
                title=f"Proposed task {i}",
            )
            for i in range(20)
        ]

        result = summarize_tasks(entries)

        active_lines = [l for l in result.splitlines() if "→" in l]
        self.assertEqual(len(active_lines), 15)
        self.assertIn("(... 5 more)", result)


if __name__ == "__main__":
    unittest.main()
