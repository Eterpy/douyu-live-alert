from contextlib import closing
from dataclasses import replace

import pytest

from live_alert.config import Config
from live_alert.douyu import Observation
from live_alert.mail import process_one
from live_alert.state import MonitorState
from live_alert.storage import connect, initialize

ROOM = 10001


def config(**changes) -> Config:
    """Test defaults: a fictional room and example.org mail addresses."""
    base = {
        "room_id": ROOM,
        "smtp_host": "smtp.example.org",
        "smtp_from_address": "alerts@example.org",
        "smtp_to_address": "user@example.org",
    }
    return Config(**{**base, **changes})


class Scenario:
    def __init__(self, c, db):
        self.c, self.db = c, db
        self.t = 1000
        self.state = MonitorState(db, c, self.t, lambda: self.t)
        self.sent = []

    def feed(self, value, delta=0, **fields):
        self.t += delta
        item = Observation(value, "fixture", self.t, self.t, self.t, **fields)
        self.state.observe(item, "test", self.t)
        return item

    def baseline(self, value="OFFLINE"):
        self.feed(value)
        self.feed(value, 10)

    def live(self):
        self.feed("LIVE", 60)
        self.feed("LIVE", 10)

    def offline(self):
        self.feed("OFFLINE", 60)
        self.feed("REPLAY", 60)
        self.feed("OFFLINE", 60)

    def restart(self, delay=0, **changes):
        self.t += delay
        self.c = replace(self.c, **changes)
        initialize(self.db, self.c)
        self.state = MonitorState(self.db, self.c, self.t, lambda: self.t)

    def events(self):
        return [
            tuple(row)
            for row in self.db.execute("SELECT kind,state FROM outbox ORDER BY id")
        ]

    def worker(self, result="SENT"):
        def transport(c, msg, before_data):
            before_data()
            self.sent.append(msg)
            return result, result

        return process_one(self.c, transport, lambda: self.t)


@pytest.fixture
def scenario(tmp_path):
    c = config(data_dir=str(tmp_path), notification_mode="send")
    with closing(connect(c.db)) as db:
        initialize(db, c)
        yield Scenario(c, db)


@pytest.fixture(autouse=True)
def restore_umask():
    # cli.main() tightens the process umask; keep tests independent of order.
    import os

    previous = os.umask(0o022)
    os.umask(previous)
    yield
    os.umask(previous)


@pytest.fixture(autouse=True)
def offline_network(monkeypatch):
    import socket

    from aiodouyu import fetch_room

    monkeypatch.setattr("live_alert.douyu.fetch_room", fetch_room)

    def forbidden(*args, **kwargs):
        pytest.fail("离线用例调用了真实网络或监听端口")

    for target, name in (
        (socket.socket, "connect"),
        (socket.socket, "bind"),
        (socket.socket, "listen"),
        (socket, "getaddrinfo"),
    ):
        monkeypatch.setattr(target, name, forbidden)
