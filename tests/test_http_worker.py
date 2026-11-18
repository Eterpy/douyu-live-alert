import json
import sys
from email.message import Message
from io import BytesIO
from unittest.mock import patch
from urllib.request import OpenerDirector, Request

import pytest

from live_alert import http_worker


@pytest.mark.parametrize(
    "declared,body,expected_reads,expected",
    [
        (None, b'{"rid":3}', [10], {"room_id": 3}),
        (None, b"no match", [10], {"room_id": None}),
        ("99", b"", [], {"error": "ResponseTooLarge", "code": None}),
        (None, b"0123456789", [10], {"error": "ResponseTooLarge", "code": None}),
        ("9", b"short", [10], {"error": "IncompleteRead", "code": None}),
    ],
)
def test_resolve_worker_bounds_the_mobile_response(
    monkeypatch, capsys, declared, body, expected_reads, expected
):
    reads = []

    class Reply(BytesIO):
        headers = Message()

        def read(self, n=-1):
            reads.append(n)
            return super().read(n)

    reply = Reply(body)
    if declared is not None:
        reply.headers["Content-Length"] = declared
    monkeypatch.setattr(http_worker, "MAX_RESPONSE_BYTES", 9)
    monkeypatch.setattr(sys, "argv", ["worker", "42", "2", "resolve"])
    with patch.object(OpenerDirector, "open", return_value=reply):
        http_worker.main()
    assert json.loads(capsys.readouterr().out) == expected
    assert reads == expected_reads and reply.closed


@pytest.mark.parametrize(
    "html,expected",
    [
        ("<html>Maintenance</html>", None),
        ('{"rid":0}', None),
        ('{"room_id":"42oops"}', None),
        ('{"room_id":"42"}', "42"),
        ('{"rid":123}', "123"),
    ],
)
def test_resolve_cli_needs_a_valid_room_id_on_the_page(
    monkeypatch, capsys, html, expected
):
    import asyncio

    from live_alert.cli import main

    # Exercise the real worker protocol, replacing only its outbound HTTP read.
    spawn = asyncio.create_subprocess_exec
    child = (
        "import sys; from live_alert import http_worker; "
        "html = sys.argv.pop(); http_worker._read_text = lambda *_: html; "
        "http_worker.main()"
    )

    async def offline_worker(
        executable, safe_path, module_flag, module, *args, **pipes
    ):
        return await spawn(executable, safe_path, "-c", child, *args, html, **pipes)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", offline_worker)
    with patch.dict("os.environ", {}, clear=True):
        code = main(["resolve-room", "42"])
    out, err = capsys.readouterr()
    if expected is None:
        assert code != 0 and out == "" and "房间号" in err
    else:
        assert code == 0 and out.strip() == expected


def test_redirect_is_followed_without_reading_the_redirect_body():
    class RedirectBody(BytesIO):
        def read(self, n=-1):
            pytest.fail("redirect body was read")

    handler = http_worker._SkipRedirectBody()
    handler.add_parent(OpenerDirector())
    req = Request("https://www.douyu.com/old")
    req.timeout = 2
    headers = Message()
    headers["Location"] = "/new"
    body = RedirectBody()
    with patch.object(handler.parent, "open") as follow:
        handler.http_error_307(req, body, 307, "redirect", headers)
    assert body.closed
    assert follow.call_args.args[0].full_url == "https://www.douyu.com/new"


def test_worker_ignores_modules_in_working_directory(tmp_path, monkeypatch):
    import asyncio

    # A planted stdlib name in the working directory must not run in the worker.
    (tmp_path / "json.py").write_text("raise SystemExit('shadowed')")
    monkeypatch.chdir(tmp_path)
    # room_id 0 fails validation inside the real worker, before any network use.
    with pytest.raises(http_worker.WorkerError, match="^ValueError$"):
        asyncio.run(http_worker._call_worker(0, 10, "betard"))
