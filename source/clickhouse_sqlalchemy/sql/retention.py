from sqlalchemy import and_, bindparam, func, literal_column

from .. import types
from .selectable import select


__all__ = ('channel_retention',)


# ClickHouse timezone arguments must be constant strings, not bind
# parameters. Render 'UTC' inline.
_UTC = literal_column("'UTC'")


def channel_retention(
        registrations,
        events,
        user_id,
        registered_at,
        channel,
        event_user_id,
        event_time,
        max_day=7,
        snapshot_as_of=None,
        cohort_start=None,
        cohort_end=None
):
    """
    Build a channel retention query as a composable
    :class:`~clickhouse_sqlalchemy.sql.selectable.Select`.

    Users are grouped by their UTC registration day and channel. For
    every cohort the query reports day 0 .. ``max_day`` retention: the
    cohort size (denominator), how many cohort users were active again
    exactly ``day_offset`` days after their registration day, and the
    retention rate.

    Recently registered users have not been observed for all days yet
    and must not be counted as lost. A cell is *mature* only when its
    whole UTC day has fully elapsed before ``snapshot_as_of``. Immature
    cells are flagged with ``is_mature = 0`` and return ``NULL`` for
    ``retained_users`` and ``retention_rate``. The denominator
    ``cohort_size`` is reported only for cohorts whose registration day
    has fully elapsed before the snapshot. Mature cells without any
    activity return 0.

    Everything is computed inside ClickHouse in a single statement. The
    day grid is built from per-cohort aggregates (cohorts x days), not
    from a per-user cross join with all days.

    :param registrations: selectable (table, alias or subquery) with
        exactly one row per user.
    :param events: selectable with behavior detail rows.
    :param user_id: column of ``registrations`` uniquely identifying a
        user.
    :param registered_at: column of ``registrations`` with the
        registration timestamp.
    :param channel: column of ``registrations`` with the acquisition
        channel.
    :param event_user_id: column of ``events`` identifying the acting
        user. Must be joinable to ``user_id`` (same type).
    :param event_time: column of ``events`` with the event timestamp.
        Events earlier than the user's ``registered_at`` (including
        earlier the same UTC day) are ignored; a user is counted at
        most once per activity day.
    :param max_day: last retention day to report (days 0 .. ``max_day``).
        Bound as the ``retention_max_day`` parameter.
    :param snapshot_as_of: report cutoff moment. Bound as the
        ``retention_snapshot`` parameter; may be omitted here and
        supplied at execution time instead.
    :param cohort_start: optional first cohort date (inclusive). Bound
        as the ``retention_cohort_start`` parameter.
    :param cohort_end: optional last cohort date (inclusive). Bound as
        the ``retention_cohort_end`` parameter.
    :return: ``Select`` with columns ``cohort_date``, ``channel``,
        ``day_offset``, ``cohort_size``, ``retained_users``,
        ``retention_rate`` and ``is_mature``.

    Example::

        query = channel_retention(
            users, events,
            user_id=users.c.user_id,
            registered_at=users.c.registered_at,
            channel=users.c.channel,
            event_user_id=events.c.user_id,
            event_time=events.c.event_time,
            max_day=7,
            snapshot_as_of=datetime(2024, 6, 1)
        )
        # Composable: order, limit, wrap in a subquery, etc.
        query = query.order_by(
            query.selected_columns.cohort_date,
            query.selected_columns.channel,
            query.selected_columns.day_offset
        )
        session.execute(query, {'retention_snapshot': datetime.utcnow()})
    """
    if isinstance(max_day, int) and max_day < 0:
        raise ValueError('max_day must be non-negative')

    max_day_param = bindparam(
        'retention_max_day', value=max_day, type_=types.Int32
    )
    snapshot_param = bindparam(
        'retention_snapshot', value=snapshot_as_of, type_=types.DateTime
    )

    # One row per registered user. Cohort day is the UTC registration day.
    cohorts = select(
        user_id.label('user_id'),
        func.toDate(registered_at, _UTC, type_=types.Date).label(
            'cohort_date'
        ),
        channel.label('channel'),
        registered_at.label('registered_at')
    ).select_from(registrations)

    if cohort_start is not None:
        cohorts = cohorts.where(
            func.toDate(registered_at, _UTC) >= bindparam(
                'retention_cohort_start', value=cohort_start,
                type_=types.Date
            )
        )
    if cohort_end is not None:
        cohorts = cohorts.where(
            func.toDate(registered_at, _UTC) <= bindparam(
                'retention_cohort_end', value=cohort_end, type_=types.Date
            )
        )

    cohorts = cohorts.subquery('cohorts')

    # Cohort sizes (denominators).
    denominators = select(
        cohorts.c.cohort_date,
        cohorts.c.channel,
        func.uniqExact(cohorts.c.user_id, type_=types.UInt64).label(
            'cohort_size'
        )
    ).select_from(cohorts).group_by(
        cohorts.c.cohort_date, cohorts.c.channel
    ).subquery('denominators')

    # (cohort_date, channel) x (day 0..max_day) cells. The grid is built
    # from cohort aggregates, never from a per-user cross join with days.
    grid = select(
        denominators.c.cohort_date,
        denominators.c.channel,
        denominators.c.cohort_size,
        func.toInt32(
            func.arrayJoin(func.range(max_day_param + 1)),
            type_=types.Int32
        ).label('day_offset')
    ).select_from(denominators).subquery('grid')

    # A cell is mature only when its whole UTC day has fully elapsed
    # before the snapshot. The denominator is final once the
    # registration day itself has fully elapsed.
    cells = select(
        grid.c.cohort_date,
        grid.c.channel,
        grid.c.cohort_size,
        grid.c.day_offset,
        (
            func.toDateTime(
                func.addDays(grid.c.cohort_date, grid.c.day_offset + 1),
                _UTC
            ) <= snapshot_param
        ).label('is_mature'),
        (
            func.toDateTime(
                func.addDays(grid.c.cohort_date, 1), _UTC
            ) <= snapshot_param
        ).label('is_cohort_complete')
    ).select_from(grid).subquery('cells')

    # Sparse retained counts: only (user, activity day) pairs that
    # actually happened. Events before the registration moment (e.g.
    # earlier on the registration day) do not count.
    activity_date = func.toDate(event_time, _UTC, type_=types.Date)
    retained = select(
        cohorts.c.cohort_date,
        cohorts.c.channel,
        func.toInt32(
            activity_date - cohorts.c.cohort_date,
            type_=types.Int32
        ).label('day_offset'),
        func.uniqExact(event_user_id, type_=types.UInt64).label(
            'retained_users'
        )
    ).select_from(
        cohorts.join(events, event_user_id == cohorts.c.user_id)
    ).where(
        event_time >= cohorts.c.registered_at,
        activity_date <= func.addDays(cohorts.c.cohort_date, max_day_param)
    ).group_by(
        cohorts.c.cohort_date,
        cohorts.c.channel,
        activity_date - cohorts.c.cohort_date
    ).subquery('retained')

    retained_users = func.coalesce(
        retained.c.retained_users, 0, type_=types.UInt64
    )

    # NB: divide() instead of the / operator. SQLAlchemy renders / as
    # "CAST(... AS Decimal(None, None))" for div_is_floordiv dialects,
    # which is not valid ClickHouse SQL; ClickHouse divide() is always
    # a true (Float64) division.
    return select(
        cells.c.cohort_date.label('cohort_date'),
        cells.c.channel.label('channel'),
        cells.c.day_offset.label('day_offset'),
        func.if_(
            cells.c.is_cohort_complete, cells.c.cohort_size, None,
            type_=types.UInt64
        ).label('cohort_size'),
        func.if_(
            cells.c.is_mature, retained_users, None,
            type_=types.UInt64
        ).label('retained_users'),
        func.if_(
            cells.c.is_mature,
            func.divide(
                retained_users, cells.c.cohort_size, type_=types.Float64
            ),
            None,
            type_=types.Float64
        ).label('retention_rate'),
        cells.c.is_mature.label('is_mature')
    ).select_from(cells).join(
        retained,
        and_(
            retained.c.cohort_date == cells.c.cohort_date,
            retained.c.channel == cells.c.channel,
            retained.c.day_offset == cells.c.day_offset
        ),
        isouter=True
    )
