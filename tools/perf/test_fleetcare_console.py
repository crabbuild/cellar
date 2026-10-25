# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import pathlib
import socket
import tempfile
import unittest

from fleetcare_console import available_port, private_file


class ConsoleLabGuards(unittest.TestCase):
    def test_console_port_leaves_room_for_reserved_ports(self):
        for port in (1023, 65526):
            with self.subTest(port=port), self.assertRaises(RuntimeError):
                available_port(port, fallback=False)

    def test_explicit_port_refuses_an_existing_listener(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
            with self.assertRaises(RuntimeError):
                available_port(port, fallback=False)

    def test_automatic_port_skips_an_occupied_block(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
            if port > 65425:
                self.skipTest("ephemeral listener leaves no room for a second block")
            selected = available_port(port, fallback=True)
            self.assertGreater(selected, port)
            self.assertEqual((selected - port) % 100, 0)

    def test_rewriting_credentials_removes_broad_file_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "credentials"
            path.write_text("old fixture")
            path.chmod(0o644)
            private_file(path, "new fixture")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(path.read_text(), "new fixture")
