import datetime


def timedelta_to_steps(
    delta: datetime.timedelta | list[datetime.timedelta] | tuple[datetime.timedelta, ...],
    time_step_unit: datetime.timedelta
) -> int | list[int]:
    convert = lambda d: max(1, int(d.total_seconds() / time_step_unit.total_seconds()))
    if isinstance(delta, (list, tuple)):
        return [convert(d) for d in delta]
    return convert(delta)


# Convert hours → steps
def hours_to_steps(
    hours: int | float | list[int | float] | tuple[int | float],
    time_step_unit: datetime.timedelta
) -> int | list[int] | tuple[int]:
    return timedelta_to_steps(datetime.timedelta(hours=hours), time_step_unit)
