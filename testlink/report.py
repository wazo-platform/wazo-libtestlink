# -*- coding: utf-8 -*-
# Copyright (C) 2016 The Wazo Authors  (see AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

from __future__ import unicode_literals

import itertools
import re
import json

from docutils.core import publish_string
from datetime import datetime


def generate_report(report, output='html'):
    if output == 'json':
        return json.dumps(report)

    rst = generate_rst(report)
    if output == 'rst':
        return rst

    return publish_string(rst, writer_name=output,
                          settings_overrides={'input_encoding': 'unicode'})


def generate_rst(report):
    lines = []

    title = 'Test report for Wazo {version}'.format(version=report['version'])
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    timestamp = 'Report generated at {date}'.format(date=now)

    lines.extend(_build_title(title, '='))
    lines.append(timestamp)
    lines.append('')
    lines.extend(_build_title('Totals'))
    lines.extend(_build_totals(report['tests']))
    lines.append('')
    lines.extend(_build_title('Tests'))

    for folder, executions in report['tests']:
        lines.extend(_build_title(folder, '_'))
        lines.extend(_build_table(executions))
        lines.append('')

    markup = '\n'.join(lines)
    return markup


def _build_title(folder, underline='-'):
    yield _escape_markup(folder)
    yield underline * len(folder)
    yield ''


def _build_totals(tests):
    totals = {'passed': 0,
              'blocked': 0,
              'failed': 0}

    executions = itertools.chain.from_iterable(x[1] for x in tests)
    for execution in executions:
        status = execution['status']
        totals[status] += 1

    for name, count in sorted(totals.iteritems(), key=lambda x: x[0]):
        yield ':{status}: {count}'.format(status=name.capitalize(),
                                          count=count)


def _build_table(executions):
    table_row = "{name} {status}"
    name_template = "X-{e[number]}: {e[name]} (v{e[version]})"

    formatted_names = [_escape_markup(name_template.format(e=e)) for e in executions]
    formatted_statuses = [_escape_markup(e['status'].capitalize()) for e in executions]

    max_name = max(len(n) for n in formatted_names)
    max_status = max(len(s) for s in formatted_statuses)

    title_row = table_row.format(name='Test'.ljust(max_name),
                                 status='Status'.ljust(max_status))
    bar = table_row.format(name='=' * max_name,
                           status='=' * max_status)

    yield bar
    yield title_row
    yield bar

    for name, status in zip(formatted_names, formatted_statuses):
        yield table_row.format(name=name.ljust(max_name),
                               status=status.ljust(max_status))

    yield bar


def _escape_markup(text):
    replace = lambda m: '\\' + m.group(1)
    return re.sub(r'([*`|_])', replace, text)
