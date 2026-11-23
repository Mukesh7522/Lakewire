from lakewire import spark_utils


def test_spark_tolerates_the_host_sleeping():
    """A sleeping laptop jumps the wall clock. With Spark's 120 s default the driver killed its own executor."""
    assert spark_utils.NETWORK_TIMEOUT_SECONDS >= 3600
