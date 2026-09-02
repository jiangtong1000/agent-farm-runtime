import unittest

from agent_farm_runtime.models import Lease, Task, TaskState
from agent_farm_runtime.transitions import TransitionError, transition_task


class TransitionTests(unittest.TestCase):
    def test_ready_to_running(self):
        task = Task("T1", "x", "out", "check")
        lease = Lease("w1", "L1")
        out = transition_task(task, TaskState.RUNNING, new_lease=lease)
        self.assertEqual(out.state, TaskState.RUNNING)
        self.assertEqual(out.lease, lease)

    def test_illegal_ready_to_done(self):
        task = Task("T1", "x", "out", "check")
        with self.assertRaises(TransitionError):
            transition_task(task, TaskState.DONE, acceptance_recorded=True)

    def test_done_requires_acceptance(self):
        task = Task("T1", "x", "out", "check", state=TaskState.SUBMITTED)
        with self.assertRaises(TransitionError):
            transition_task(task, TaskState.DONE)
        done = transition_task(task, TaskState.DONE, acceptance_recorded=True)
        self.assertEqual(done.state, TaskState.DONE)

    def test_stale_lease_rejected(self):
        task = Task("T1", "x", "out", "check", state=TaskState.RUNNING, lease=Lease("w1", "L-new"))
        with self.assertRaises(TransitionError):
            transition_task(task, TaskState.WAITING, lease_id="L-old")
        waiting = transition_task(task, TaskState.WAITING, lease_id="L-new", metadata_patch={"waiting_on": "job:123"})
        self.assertEqual(waiting.state, TaskState.WAITING)


if __name__ == "__main__":
    unittest.main()
