from datetime import date, datetime

from sqlalchemy import Column

from clickhouse_sqlalchemy import types, Table, engines, select, \
    channel_retention
from tests.testcase import CompilationTestCase


class ChannelRetentionTestCase(CompilationTestCase):
    def _make_tables(self):
        metadata = self.metadata()
        users = Table(
            'users', metadata,
            Column('user_id', types.UInt64),
            Column('registered_at', types.DateTime),
            Column('channel', types.String),
            engines.Memory(),
        )
        events = Table(
            'events', metadata,
            Column('user_id', types.UInt64),
            Column('event_time', types.DateTime),
            engines.Memory(),
        )
        return users, events

    def _make_query(self, users, events, **kwargs):
        return channel_retention(
            users, events,
            user_id=users.c.user_id,
            registered_at=users.c.registered_at,
            channel=users.c.channel,
            event_user_id=events.c.user_id,
            event_time=events.c.event_time,
            **kwargs
        )

    def test_default_query(self):
        users, events = self._make_tables()
        query = self._make_query(
            users, events,
            max_day=7,
            snapshot_as_of=datetime(2024, 6, 10, 12, 0, 0)
        )
        self.assertEqual(
            self.compile(query),
            'SELECT cells.cohort_date AS cohort_date, cells.channel AS '
            'channel, cells.day_offset AS day_offset, '
            'if(cells.is_cohort_complete, cells.cohort_size, NULL) AS '
            'cohort_size, if(cells.is_mature, '
            'coalesce(retained.retained_users, %(coalesce_1)s), NULL) AS '
            'retained_users, if(cells.is_mature, '
            'divide(coalesce(retained.retained_users, %(coalesce_1)s), '
            'cells.cohort_size), NULL) AS retention_rate, cells.is_mature AS '
            'is_mature FROM (SELECT grid.cohort_date AS cohort_date, '
            'grid.channel AS channel, grid.cohort_size AS cohort_size, '
            'grid.day_offset AS day_offset, '
            'toDateTime(addDays(grid.cohort_date, grid.day_offset + '
            "%(day_offset_1)s), 'UTC') <= %(retention_snapshot)s AS "
            'is_mature, toDateTime(addDays(grid.cohort_date, %(addDays_1)s), '
            "'UTC') <= %(retention_snapshot)s AS is_cohort_complete FROM "
            '(SELECT denominators.cohort_date AS cohort_date, '
            'denominators.channel AS channel, denominators.cohort_size AS '
            'cohort_size, toInt32(arrayJoin(range(%(retention_max_day)s + '
            '%(param_1)s))) AS day_offset FROM (SELECT cohorts.cohort_date AS '
            'cohort_date, cohorts.channel AS channel, '
            'uniqExact(cohorts.user_id) AS cohort_size FROM (SELECT '
            "users.user_id AS user_id, toDate(users.registered_at, 'UTC') AS "
            'cohort_date, users.channel AS channel, users.registered_at AS '
            'registered_at FROM users) AS cohorts GROUP BY '
            'cohorts.cohort_date, cohorts.channel) AS denominators) AS grid) '
            'AS cells LEFT OUTER JOIN (SELECT cohorts.cohort_date AS '
            'cohort_date, cohorts.channel AS channel, '
            "toInt32(toDate(events.event_time, 'UTC') - cohorts.cohort_date) "
            'AS day_offset, uniqExact(events.user_id) AS retained_users FROM '
            '(SELECT users.user_id AS user_id, toDate(users.registered_at, '
            "'UTC') AS cohort_date, users.channel AS channel, "
            'users.registered_at AS registered_at FROM users) AS cohorts '
            'INNER JOIN events ON events.user_id = cohorts.user_id WHERE '
            'events.event_time >= cohorts.registered_at AND '
            "toDate(events.event_time, 'UTC') <= addDays(cohorts.cohort_date, "
            '%(retention_max_day)s) GROUP BY cohorts.cohort_date, '
            "cohorts.channel, toDate(events.event_time, 'UTC') - "
            'cohorts.cohort_date) AS retained ON retained.cohort_date = '
            'cells.cohort_date AND retained.channel = cells.channel AND '
            'retained.day_offset = cells.day_offset'
        )

    def test_default_query_literal_binds(self):
        users, events = self._make_tables()
        query = self._make_query(
            users, events,
            max_day=7,
            snapshot_as_of=datetime(2024, 6, 10, 12, 0, 0)
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT cells.cohort_date AS cohort_date, cells.channel AS '
            'channel, cells.day_offset AS day_offset, '
            'if(cells.is_cohort_complete, cells.cohort_size, NULL) AS '
            'cohort_size, if(cells.is_mature, '
            'coalesce(retained.retained_users, 0), NULL) AS retained_users, '
            'if(cells.is_mature, divide(coalesce(retained.retained_users, 0), '
            'cells.cohort_size), NULL) AS retention_rate, cells.is_mature AS '
            'is_mature FROM (SELECT grid.cohort_date AS cohort_date, '
            'grid.channel AS channel, grid.cohort_size AS cohort_size, '
            'grid.day_offset AS day_offset, '
            'toDateTime(addDays(grid.cohort_date, grid.day_offset + 1), '
            "'UTC') <= '2024-06-10 12:00:00' AS is_mature, "
            "toDateTime(addDays(grid.cohort_date, 1), 'UTC') <= '2024-06-10 "
            "12:00:00' AS is_cohort_complete FROM (SELECT "
            'denominators.cohort_date AS cohort_date, denominators.channel AS '
            'channel, denominators.cohort_size AS cohort_size, '
            'toInt32(arrayJoin(range(7 + 1))) AS day_offset FROM (SELECT '
            'cohorts.cohort_date AS cohort_date, cohorts.channel AS channel, '
            'uniqExact(cohorts.user_id) AS cohort_size FROM (SELECT '
            "users.user_id AS user_id, toDate(users.registered_at, 'UTC') AS "
            'cohort_date, users.channel AS channel, users.registered_at AS '
            'registered_at FROM users) AS cohorts GROUP BY '
            'cohorts.cohort_date, cohorts.channel) AS denominators) AS grid) '
            'AS cells LEFT OUTER JOIN (SELECT cohorts.cohort_date AS '
            'cohort_date, cohorts.channel AS channel, '
            "toInt32(toDate(events.event_time, 'UTC') - cohorts.cohort_date) "
            'AS day_offset, uniqExact(events.user_id) AS retained_users FROM '
            '(SELECT users.user_id AS user_id, toDate(users.registered_at, '
            "'UTC') AS cohort_date, users.channel AS channel, "
            'users.registered_at AS registered_at FROM users) AS cohorts '
            'INNER JOIN events ON events.user_id = cohorts.user_id WHERE '
            'events.event_time >= cohorts.registered_at AND '
            "toDate(events.event_time, 'UTC') <= addDays(cohorts.cohort_date, "
            '7) GROUP BY cohorts.cohort_date, cohorts.channel, '
            "toDate(events.event_time, 'UTC') - cohorts.cohort_date) AS "
            'retained ON retained.cohort_date = cells.cohort_date AND '
            'retained.channel = cells.channel AND retained.day_offset = '
            'cells.day_offset'
        )

    def test_cohort_bounds(self):
        users, events = self._make_tables()
        query = self._make_query(
            users, events,
            max_day=3,
            snapshot_as_of=datetime(2024, 6, 10),
            cohort_start=date(2024, 5, 1),
            cohort_end=date(2024, 5, 31)
        )
        self.assertEqual(
            self.compile(query, literal_binds=True),
            'SELECT cells.cohort_date AS cohort_date, cells.channel AS '
            'channel, cells.day_offset AS day_offset, '
            'if(cells.is_cohort_complete, cells.cohort_size, NULL) AS '
            'cohort_size, if(cells.is_mature, '
            'coalesce(retained.retained_users, 0), NULL) AS retained_users, '
            'if(cells.is_mature, divide(coalesce(retained.retained_users, 0), '
            'cells.cohort_size), NULL) AS retention_rate, cells.is_mature AS '
            'is_mature FROM (SELECT grid.cohort_date AS cohort_date, '
            'grid.channel AS channel, grid.cohort_size AS cohort_size, '
            'grid.day_offset AS day_offset, '
            'toDateTime(addDays(grid.cohort_date, grid.day_offset + 1), '
            "'UTC') <= '2024-06-10 00:00:00' AS is_mature, "
            "toDateTime(addDays(grid.cohort_date, 1), 'UTC') <= '2024-06-10 "
            "00:00:00' AS is_cohort_complete FROM (SELECT "
            'denominators.cohort_date AS cohort_date, denominators.channel AS '
            'channel, denominators.cohort_size AS cohort_size, '
            'toInt32(arrayJoin(range(3 + 1))) AS day_offset FROM (SELECT '
            'cohorts.cohort_date AS cohort_date, cohorts.channel AS channel, '
            'uniqExact(cohorts.user_id) AS cohort_size FROM (SELECT '
            "users.user_id AS user_id, toDate(users.registered_at, 'UTC') AS "
            'cohort_date, users.channel AS channel, users.registered_at AS '
            'registered_at FROM users WHERE toDate(users.registered_at, '
            "'UTC') >= '2024-05-01' AND toDate(users.registered_at, 'UTC') <= "
            "'2024-05-31') AS cohorts GROUP BY cohorts.cohort_date, "
            'cohorts.channel) AS denominators) AS grid) AS cells LEFT OUTER '
            'JOIN (SELECT cohorts.cohort_date AS cohort_date, cohorts.channel '
            "AS channel, toInt32(toDate(events.event_time, 'UTC') - "
            'cohorts.cohort_date) AS day_offset, uniqExact(events.user_id) AS '
            'retained_users FROM (SELECT users.user_id AS user_id, '
            "toDate(users.registered_at, 'UTC') AS cohort_date, users.channel "
            'AS channel, users.registered_at AS registered_at FROM users '
            "WHERE toDate(users.registered_at, 'UTC') >= '2024-05-01' AND "
            "toDate(users.registered_at, 'UTC') <= '2024-05-31') AS cohorts "
            'INNER JOIN events ON events.user_id = cohorts.user_id WHERE '
            'events.event_time >= cohorts.registered_at AND '
            "toDate(events.event_time, 'UTC') <= addDays(cohorts.cohort_date, "
            '3) GROUP BY cohorts.cohort_date, cohorts.channel, '
            "toDate(events.event_time, 'UTC') - cohorts.cohort_date) AS "
            'retained ON retained.cohort_date = cells.cohort_date AND '
            'retained.channel = cells.channel AND retained.day_offset = '
            'cells.day_offset'
        )

    def test_boundaries_are_bindable(self):
        users, events = self._make_tables()
        query = self._make_query(
            users, events,
            max_day=7,
            snapshot_as_of=datetime(2024, 6, 10, 12, 0, 0),
            cohort_start=date(2024, 5, 1),
            cohort_end=date(2024, 5, 31)
        )
        params = self._compile(query).params
        self.assertEqual(params['retention_max_day'], 7)
        self.assertEqual(
            params['retention_snapshot'], datetime(2024, 6, 10, 12, 0, 0)
        )
        self.assertEqual(params['retention_cohort_start'], date(2024, 5, 1))
        self.assertEqual(params['retention_cohort_end'], date(2024, 5, 31))

    def test_composable(self):
        users, events = self._make_tables()
        query = self._make_query(
            users, events,
            max_day=7,
            snapshot_as_of=datetime(2024, 6, 10, 12, 0, 0)
        )

        ordered = query.order_by(
            'cohort_date', 'channel', 'day_offset'
        ).limit(100)
        expected_tail = (
            ' ORDER BY cohort_date, channel, day_offset  LIMIT %(param_2)s'
        )
        self.assertTrue(self.compile(ordered).endswith(expected_tail))

        # wraps into an outer query as a plain subquery
        wrapped = select(query.subquery())
        self.assertEqual(
            self.compile(wrapped),
            'SELECT anon_1.cohort_date, anon_1.channel, anon_1.day_offset, '
            'anon_1.cohort_size, anon_1.retained_users, '
            'anon_1.retention_rate, anon_1.is_mature FROM (SELECT '
            'cells.cohort_date AS cohort_date, cells.channel AS channel, '
            'cells.day_offset AS day_offset, if(cells.is_cohort_complete, '
            'cells.cohort_size, NULL) AS cohort_size, if(cells.is_mature, '
            'coalesce(retained.retained_users, %(coalesce_1)s), NULL) AS '
            'retained_users, if(cells.is_mature, '
            'divide(coalesce(retained.retained_users, %(coalesce_1)s), '
            'cells.cohort_size), NULL) AS retention_rate, cells.is_mature AS '
            'is_mature FROM (SELECT grid.cohort_date AS cohort_date, '
            'grid.channel AS channel, grid.cohort_size AS cohort_size, '
            'grid.day_offset AS day_offset, '
            'toDateTime(addDays(grid.cohort_date, grid.day_offset + '
            "%(day_offset_1)s), 'UTC') <= %(retention_snapshot)s AS "
            'is_mature, toDateTime(addDays(grid.cohort_date, %(addDays_1)s), '
            "'UTC') <= %(retention_snapshot)s AS is_cohort_complete FROM "
            '(SELECT denominators.cohort_date AS cohort_date, '
            'denominators.channel AS channel, denominators.cohort_size AS '
            'cohort_size, toInt32(arrayJoin(range(%(retention_max_day)s + '
            '%(param_1)s))) AS day_offset FROM (SELECT cohorts.cohort_date AS '
            'cohort_date, cohorts.channel AS channel, '
            'uniqExact(cohorts.user_id) AS cohort_size FROM (SELECT '
            "users.user_id AS user_id, toDate(users.registered_at, 'UTC') AS "
            'cohort_date, users.channel AS channel, users.registered_at AS '
            'registered_at FROM users) AS cohorts GROUP BY '
            'cohorts.cohort_date, cohorts.channel) AS denominators) AS grid) '
            'AS cells LEFT OUTER JOIN (SELECT cohorts.cohort_date AS '
            'cohort_date, cohorts.channel AS channel, '
            "toInt32(toDate(events.event_time, 'UTC') - cohorts.cohort_date) "
            'AS day_offset, uniqExact(events.user_id) AS retained_users FROM '
            '(SELECT users.user_id AS user_id, toDate(users.registered_at, '
            "'UTC') AS cohort_date, users.channel AS channel, "
            'users.registered_at AS registered_at FROM users) AS cohorts '
            'INNER JOIN events ON events.user_id = cohorts.user_id WHERE '
            'events.event_time >= cohorts.registered_at AND '
            "toDate(events.event_time, 'UTC') <= addDays(cohorts.cohort_date, "
            '%(retention_max_day)s) GROUP BY cohorts.cohort_date, '
            "cohorts.channel, toDate(events.event_time, 'UTC') - "
            'cohorts.cohort_date) AS retained ON retained.cohort_date = '
            'cells.cohort_date AND retained.channel = cells.channel AND '
            'retained.day_offset = cells.day_offset) AS anon_1'
        )

    def test_negative_max_day(self):
        users, events = self._make_tables()
        with self.assertRaises(ValueError):
            self._make_query(users, events, max_day=-1)
