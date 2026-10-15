"""Reject fabricated first-send boundaries and repair-before-audit evidence."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

from before_send import PAYLOAD, TOKEN, audit_records, inspect_crash_image, validate_boundary, validate_crash_image, validate_snapshot
from faults import AuditError, digest


def fixture():
    receipt = dict(payload_sha256=digest(PAYLOAD), source="Order:"+"a"*64,
                   incarnation="0"*32, lane="b"*64, sequence="1")
    revision = dict(source=receipt["source"], incarnation=receipt["incarnation"], lane=receipt["lane"],
                    next_sequence="2", settled_through="0", dispatch_generation="0")
    outbox = dict(incarnation=receipt["incarnation"], pending_rows="1", next=None,
                  lanes=[dict(pending_rows="1", attempts="0", revision=revision)])
    phase = dict(boundary="source_proven_before_send", token=TOKEN,
                 command=dict(status="created", order=PAYLOAD, receipt=receipt), outbox=outbox)
    state = dict(source=dict(order=PAYLOAD, receipt=receipt, outbox=copy.deepcopy(outbox)), events=dict(records=[]),
                 effects=dict(projection=dict(orders=0,total_cents=0), receipts={}), subscription=dict(checkpoint="0"))
    return phase, state


class BeforeSendOracleTests(unittest.TestCase):
    def test_source_that_already_dispatched_cannot_prove_first_send_boundary(self):
        for field in ("attempts", "dispatch_generation"):
            phase, _ = fixture()
            target = phase["outbox"]["lanes"][0]
            if field == "dispatch_generation":
                target = target["revision"]
            target[field] = "1"
            with self.assertRaisesRegex(AuditError, "already attempted"):
                validate_boundary(phase)

    def test_lost_commit_or_producer_identity_cannot_be_repaired_by_replay(self):
        for change in ("state", "receipt", "incarnation"):
            phase, state = fixture()
            if change == "state":
                state["source"]["order"] = None
            elif change == "receipt":
                state["source"]["receipt"] = dict(state["source"]["receipt"], sequence="2")
            else:
                state["source"]["outbox"]["incarnation"] = "1"*32
            with self.assertRaises(AuditError):
                validate_snapshot(phase, state)

    def test_pending_zero_without_destination_is_loss(self):
        phase, state = fixture()
        state["source"]["outbox"]["pending_rows"] = "0"
        state["source"]["outbox"]["lanes"][0]["pending_rows"] = "0"
        state["source"]["outbox"]["lanes"][0]["revision"]["settled_through"] = "1"
        with self.assertRaisesRegex(AuditError, "neither pending"):
            validate_snapshot(phase, state)

    def test_no_effect_or_checkpoint_is_permitted_before_replay(self):
        for field in ("effect", "checkpoint"):
            phase, state = fixture()
            if field == "effect":
                state["effects"]["projection"]["orders"] = 1
            else:
                state["subscription"]["checkpoint"] = "1"
            with self.assertRaisesRegex(AuditError, "effects/checkpoint"):
                validate_snapshot(phase, state)

    def test_post_kill_evidence_must_still_have_zero_attempts(self):
        phase, _ = fixture()
        image = dict(order=PAYLOAD,receipt=phase["command"]["receipt"],incarnation="0"*32,lane="b"*64,
                     sequence="1",next_sequence="2",settled_through="0",attempts="1",dispatch_generation="1",
                     rows="1",envelope_sha256="2"*64)
        with self.assertRaisesRegex(AuditError, "zero dispatch attempts"):
            validate_crash_image(phase, image)

    def test_archive_is_required_instead_of_a_live_or_new_empty_database(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(AuditError, "archived source database missing"):
                inspect_crash_image(Path(directory), "Order:first")
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_journal_requires_real_kill_and_audit_before_replay(self):
        phase, state = fixture()
        record = dict(phase["command"]["receipt"], order_id="first", event_id="v1:"+"b"*64+":1",
                      offset="1", envelope_sha256="2"*64, event=PAYLOAD)
        final = copy.deepcopy(state)
        final["source"]["outbox"]["pending_rows"] = "0"
        final["source"]["outbox"]["lanes"][0]["pending_rows"] = "0"
        final["source"]["outbox"]["lanes"][0]["revision"]["settled_through"] = "1"
        final["events"]["records"] = [record]
        archive = dict(record)
        archive.pop("event")
        final["effects"] = dict(projection=dict(orders=1,total_cents=725), receipts=dict(first=archive))
        final["subscription"]["checkpoint"] = "1"
        records = [dict(event="before_send_attempt",payload=PAYLOAD,sha256=digest(PAYLOAD)),
                   dict(event="source_proven_before_send",phase=phase,events=dict(records=[])),
                   dict(event="process_killed",label="source-before-first-send",exit_code=-9),
                   dict(event="before_send_response_interrupted"),
                   dict(event="before_send_crash_image", image=dict(order=PAYLOAD,receipt=phase["command"]["receipt"],
                       incarnation="0"*32,lane="b"*64,sequence="1",next_sequence="2",settled_through="0",
                       attempts="0",dispatch_generation="0",rows="1",envelope_sha256="2"*64)),
                   dict(event="before_send_audit",label="recovered-before-replay",snapshot=state),
                   dict(event="before_send_replay",status="duplicate",receipt=phase["command"]["receipt"]),
                   dict(event="before_send_audit",label="final",snapshot=final)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"ledger.jsonl"
            def verify(values):
                path.write_text("".join(json.dumps(value)+"\n" for value in values))
                return audit_records([json.loads(line) for line in path.read_text().splitlines()])
            self.assertEqual(verify(records), dict(audits=2,sigkills=1,effects=1))
            changed = copy.deepcopy(records)
            changed[2]["exit_code"] = 0
            with self.assertRaisesRegex(AuditError, "real source SIGKILL"):
                verify(changed)
            changed = copy.deepcopy(records)
            changed[5], changed[6] = changed[6], changed[5]
            with self.assertRaisesRegex(AuditError, "preceded audit"):
                verify(changed)


if __name__ == "__main__":
    unittest.main()
