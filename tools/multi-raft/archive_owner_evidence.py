# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Require both observed physical-reader cancellation profiles, not test exit alone."""
import json
import re

CASE = "rustfs_archive_cancelled_waiter_keeps_physical_checkpoint"
PREFIX = "ARCHIVE_PHYSICAL_OWNER_EVIDENCE "


def observations(output, case):
    if case != CASE:
        raise ValueError("unknown physical archive owner case")
    rows = []
    for value in re.findall(re.escape(PREFIX) + r"([^\n]*)", output):
        if len(value) > 8192:
            raise ValueError("oversized physical owner evidence")
        row = json.loads(value)
        if not isinstance(row, dict) or type(row.get("extents")) is not bool:
            raise ValueError("invalid physical owner profile")
        rows.append(row)
    if len(rows) != 2 or {row["extents"] for row in rows} != {False, True}:
        raise ValueError("incomplete physical owner image coverage")
    for row in rows:
        if (type(row.get("schema")) is not int or row["schema"] != 1
                or row.get("case") != CASE.removeprefix("rustfs_")
                or not isinstance(row.get("history"), str) or not 1 <= len(row["history"]) <= 1024):
            raise ValueError("invalid physical owner history/schema")
        for field in ("old_root", "new_root", "old_page_sha256", "old_image_sha256",
                      "held_page_sha256", "held_image_sha256"):
            if not isinstance(row.get(field), str) or not re.fullmatch(r"[0-9a-f]{64}", row[field]):
                raise ValueError("missing physical file/root hash")
        if (row["old_page_sha256"] != row["held_page_sha256"]
                or row["old_image_sha256"] != row["held_image_sha256"]):
            raise ValueError("held checkpoint physical bytes changed")
        if (any(type(row.get(field)) is not int or not 0 < row[field] < 2**64
                for field in ("old_position", "new_position"))
                or row["new_position"] <= row["old_position"] or row["old_root"] == row["new_root"]):
            raise ValueError("live image did not advance beyond held checkpoint")
        for field in ("old_image_bytes", "held_source_bytes", "held_archive_bytes"):
            if type(row.get(field)) is not int or not 0 < row[field] <= 32 * 1024**2:
                raise ValueError("invalid retained physical owner bytes")
        for field in ("held_sources", "held_archive_operations"):
            if type(row.get(field)) is not int or row[field] != 1:
                raise ValueError("physical source lost admission")
        for field in ("cancelled_indexes", "released_source_bytes", "released_archive_bytes"):
            if type(row.get(field)) is not int or row[field] != 0:
                raise ValueError("physical exit failed to release ownership or cancellation published an index")
        for field in ("old_page_removed", "old_image_removed", "fresh_checkpoint_verified"):
            if row.get(field) is not True:
                raise ValueError("missing physical retention/deletion/recovery proof")
    return rows
