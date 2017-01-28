import pytest


def test_iterseek_exists():
    try:
        from binlog.abstract import IterSeek
    except ImportError as exc:
        assert False, exc


def test_iterseek_is_abstract():
    from binlog.abstract import IterSeek

    with pytest.raises(TypeError):
        IterSeek()


    class MyIterSeek(IterSeek):
        def seek(self, pos):
            pass

        def __next__(self):
            pass

    # DID NOT RAISE
    MyIterSeek()
