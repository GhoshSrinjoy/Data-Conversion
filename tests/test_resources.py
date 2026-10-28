import pytest

from dataconv import resources
from dataconv._util import GiB, MiB, atomic_output, format_size, parse_size


def test_cgroup_v2(tmp_path):
    (tmp_path / "cpu.max").write_text("150000 100000\n")
    (tmp_path / "memory.max").write_text(str(2 * GiB))
    (tmp_path / "memory.current").write_text(str(512 * MiB))
    assert resources.cgroup_cpu_quota(str(tmp_path)) == 1.5
    assert resources.cgroup_memory(str(tmp_path)) == (2 * GiB, 512 * MiB)


def test_cgroup_v1_and_unlimited(tmp_path):
    (tmp_path / "cpu").mkdir()
    (tmp_path / "cpu" / "cpu.cfs_quota_us").write_text("400000")
    (tmp_path / "cpu" / "cpu.cfs_period_us").write_text("100000")
    (tmp_path / "memory").mkdir()
    (tmp_path / "memory" / "memory.limit_in_bytes").write_text(str(1 << 62))
    assert resources.cgroup_cpu_quota(str(tmp_path)) == 4.0
    assert resources.cgroup_memory(str(tmp_path)) == (None, None)
    (tmp_path / "cpu.max").write_text("max 100000")
    assert resources.cgroup_cpu_quota(str(tmp_path)) == 4.0


def fake(cpus, avail):
    return resources.SystemInfo("test", "", "x86_64", "3", "CPython", 64, True, cpus, cpus, cpus, avail * 2, avail, None, False, "test")


def test_plan_adapts(monkeypatch):
    # the machines are the fake ones, not overrides from the environment
    monkeypatch.delenv("DATACONV_THREADS", raising=False)
    monkeypatch.delenv("DATACONV_MEMORY", raising=False)
    big = resources.make_plan(100 * GiB, info=fake(32, 64 * GiB))
    assert big.threads == 32 and big.chunk_size == 64 * MiB
    tiny = resources.make_plan(10 * GiB, info=fake(1, 128 * MiB))
    assert tiny.threads == 1 and tiny.low_memory
    many_small = resources.make_plan(100 * MiB, info=fake(8, 16 * GiB), files=50)
    assert many_small.workers > 1
    forced = resources.make_plan(None, threads=3, memory="1GB", info=fake(8, 16 * GiB))
    assert (forced.threads, forced.memory_budget) == (3, GiB)


def test_detect_is_sane():
    info = resources.detect()
    assert info.cpu_usable >= 1
    assert info.mem_total >= info.mem_available > 0


def test_sizes():
    assert parse_size("64MB") == 64 * MiB
    assert parse_size("1.5g") == int(1.5 * GiB)
    assert parse_size("100", "m") == 100 * MiB
    assert parse_size(512) == 512
    assert parse_size(512, "m") == 512  # numbers are always bytes
    with pytest.raises(ValueError):
        parse_size("lots")
    assert format_size(1536) == "1.5 KB"


def test_atomic_output(tmp_path):
    target = tmp_path / "out.bin"
    with pytest.raises(RuntimeError), atomic_output(target) as tmp:
        tmp.write_bytes(b"partial")
        raise RuntimeError("boom")
    assert not target.exists() and list(tmp_path.iterdir()) == []
    with atomic_output(target) as tmp:
        tmp.write_bytes(b"done")
    assert target.read_bytes() == b"done"
    with pytest.raises(FileExistsError), atomic_output(target) as tmp:
        pass
