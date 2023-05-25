import psycopg2
import itertools
from contextlib import contextmanager

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

LOG_COLUMNS = ('folder',
               'number',
               'version',
               'name',
               'status',
               'timestamp',
               'notes',
               'firstname',
               'lastname',
               'user')


class Database:

    def __init__(self, host, port, database, user, password):
        self.connection = psycopg2.connect(host=host,
                                           port=port,
                                           database=database,
                                           user=user,
                                           password=password)

    def row(self, query, **params):
        cursor = self._cursor_for_query(query, params)
        row = cursor.fetchone()
        cursor.close()
        return row

    def scalar(self, query, **params):
        row = self.row(query, **params)
        return row[0]

    def rows(self, query, **params):
        cursor = self._cursor_for_query(query, params)
        yield from cursor
        cursor.close()

    def _cursor_for_query(self, query, params):
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        return cursor

    @contextmanager
    def transaction(self):
        try:
            yield
            self.connection.commit()
        except:
            self.connection.rollback()
            raise


class Build:

    def __init__(self, project, database):
        self.database = database
        self.project = project
        self._id = None
        self._version = None

    @property
    def id(self):
        if not self._id:
            self.refresh()
        return self._id

    @property
    def version(self):
        if not self._version:
            self.refresh()
        return self._version

    def refresh(self):
        query = """
        SELECT
            builds.id,
            builds.name
        FROM
            builds
            INNER JOIN testplans
                ON testplans.id = builds.testplan_id
                INNER JOIN testprojects
                    ON testprojects.id = testplans.testproject_id
        WHERE
            testprojects.notes = %(project)s
        ORDER BY
            builds.creation_ts DESC
        LIMIT 1
        """

        row = self.database.row(query, project=self.project)
        self._id = row[0]
        self._version = row[1]


db = None
build = None


def log_cte(latest=True, folder_order=False):
    query = """
    WITH RECURSIVE path_tree(id, name) AS
    (
        SELECT
            parent.id,
            CAST(parent.name as varchar(200)) as name
        FROM
            nodes_hierarchy parent
        WHERE
            parent.parent_id = (
                SELECT testplans.testproject_id
                FROM builds
                INNER JOIN testplans ON builds.testplan_id = testplans.id
                WHERE builds.id = %(build_id)s
            )
        UNION ALL
        SELECT
            child.id,
            CAST(path_tree.name || '/' || child.name as varchar(200)) as name
        FROM
            path_tree
            INNER JOIN nodes_hierarchy child
                ON path_tree.id = child.parent_id
                AND child.node_type_id = 2
    ),

    latest_executions AS
    (
        SELECT
            executions.tcversion_id AS tcversion_id,
            MAX(executions.execution_ts) AS execution_ts
        FROM
            executions
        GROUP BY
            executions.tcversion_id
    ),

    log_journal AS (
    SELECT
        path_tree.name                      AS folder,
        tcversions.tc_external_id           AS number,
        tcversions.version                  AS version,
        parent.name                         AS name,
        (CASE executions.status
        WHEN 'p' THEN 'passed'
        WHEN 'f' THEN 'failed'
        WHEN 'b' THEN 'blocked'
        ELSE executions.status
        END)                                AS status,
        executions.execution_ts             AS timestamp,
        executions.notes                    AS notes,
        users.first                         AS firstname,
        users.last                          AS lastname,
        users.first || ' ' || users.last    AS user
    FROM
        executions
        INNER JOIN users
            ON executions.tester_id = users.id
        INNER JOIN builds
            ON builds.id = executions.build_id
            AND builds.id = %(build_id)s
        INNER JOIN tcversions
            ON executions.tcversion_id = tcversions.id
            INNER JOIN nodes_hierarchy node
                ON tcversions.id = node.id
                INNER JOIN nodes_hierarchy parent
                    ON node.parent_id = parent.id
                    LEFT OUTER JOIN path_tree
                        ON parent.parent_id = path_tree.id
    """

    if latest:
        query += """
        INNER JOIN latest_executions
            ON executions.tcversion_id = latest_executions.tcversion_id
            AND executions.execution_ts = latest_executions.execution_ts
        """

    if folder_order:
        query += """
        ORDER BY
            path_tree.name ASC,
            parent.node_order DESC
        """

    query += ")"

    return query


def setup(**kwargs):
    global db
    db = Database(kwargs['host'],
                  kwargs['port'],
                  kwargs['database'],
                  kwargs['user'],
                  kwargs['password'])

    global build
    build = Build(kwargs['project'],
                  db)


def total_manual_tests():
    query = """
    SELECT
        count(tcversions.id)
    FROM
        tcversions
        INNER JOIN testplan_tcversions
            ON tcversions.id = testplan_tcversions.tcversion_id
            INNER JOIN builds
                ON builds.testplan_id = testplan_tcversions.testplan_id
    WHERE
        tcversions.execution_type = 1
        AND builds.id = %(build_id)s
    GROUP BY
        builds.id
    """

    return db.scalar(query, build_id=build.id)


def test_statuses():
    query = log_cte() + """
    SELECT status, COUNT(status)
    FROM log_journal
    GROUP BY status
    """

    rows = db.rows(query, build_id=build.id)
    statuses = {'passed': 0, 'failed': 0, 'blocked': 0}
    statuses.update({key: value for key, value in rows})

    return statuses


def tests_for_status(status):
    query = log_cte() + """
    SELECT number, name, notes
    FROM log_journal
    WHERE status = %(status)s
    ORDER BY number
    """

    rows = db.rows(query, build_id=build.id, status=status)

    tests = [{'name': f"X-{row[0]}: {row[1]}",
              'notes': row[2].strip()}
             for row in rows]

    return tests


def executed_per_person():
    query = log_cte() + """,
    latest_folder AS (
        SELECT
            ranks.user,
            ranks.folder
        FROM
            (
                SELECT
                    log_journal.user AS user,
                    log_journal.folder AS folder,
                    rank() OVER
                        (PARTITION BY log_journal.user ORDER BY timestamp DESC)
                        AS pos
                FROM
                    log_journal
            ) AS ranks
        WHERE
            ranks.pos = 1
    )

    SELECT
        log_journal.user            AS user,
        log_journal.status          AS status,
        latest_folder.folder        AS last_path,
        COUNT(log_journal.number)   AS executed
    FROM
        log_journal
        LEFT OUTER JOIN latest_folder
            ON latest_folder.user = log_journal.user
    GROUP BY
        log_journal.user,
        log_journal.status,
        latest_folder.folder
    """

    rows = db.rows(query, build_id=build.id)

    scores = {}
    paths = {}

    for row in rows:
        name, status, last_path, executed = row
        scores.setdefault(name, {})[status] = executed
        paths[name] = last_path

    result = [{'name': n,
               'executed': e,
               'last_path': paths[n]}
              for n, e in scores.iteritems()
              ]

    result.sort(reverse=True, key=lambda x: sum(x['executed'].values()))

    return result


def build_testers():
    scores = executed_per_person()
    return scores


def dashboard():
    with db.transaction():
        failed = tests_for_status("failed")
        blocked = tests_for_status("blocked")
        testers = build_testers()

        stats = test_statuses()
        stats['total'] = total_manual_tests()

    return {
        'version': build.version,
        'stats': stats,
        'failed': failed,
        'blocked': blocked,
        'testers': testers
    }


def log_journal(latest=False, timestamp=None, status=None,
                sort='timestamp', order='asc'):

    conditions = _build_where(timestamp, status)
    order_by = _build_order_by(sort, order)

    query = log_cte(latest=latest)
    query += "SELECT * FROM log_journal" + conditions + order_by

    with db.transaction():
        rows = db.rows(query,
                       build_id=build.id,
                       timestamp=timestamp,
                       status=status)
        return tuple({col: row[pos] for pos, col in enumerate(LOG_COLUMNS)}
                     for row in rows)


def _build_where(timestamp, status):
    conditions = []
    if timestamp:
        conditions.append("timestamp >= %(timestamp)s")
    if status:
        conditions.append("status = %(status)s")

    if conditions:
        return " WHERE " + " AND ".join(conditions)
    return ""


def _build_order_by(sort, order):
    if sort not in LOG_COLUMNS:
        raise Exception(f"unknown column '{sort}'")

    if order not in ('asc', 'desc'):
        raise Exception(f"unknown order '{order}'")

    return f' ORDER BY {sort} {order}'


def manual_test_report():
    query = log_cte(folder_order=True) + """
    SELECT
        log_journal.folder,
        log_journal.number,
        log_journal.name,
        log_journal.version,
        log_journal.status
    FROM
        log_journal
    """

    rows = db.rows(query, build_id=build.id)
    tests = group_executions_by_folder(rows)

    return {'version': build.version,
            'tests': tests}


def group_executions_by_folder(rows):
    report = []

    for folder, rows in itertools.groupby(rows, lambda r: r[0]):
        executions = tuple({'number': row[1],
                            'name': row[2],
                            'version': row[3],
                            'status': row[4]}
                           for row in rows)
        report.append((folder, executions))

    return report
