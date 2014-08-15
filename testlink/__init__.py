import dao


def setup(**kwargs):
    kwargs.setdefault('host', 'localhost')
    kwargs.setdefault('port', 5432)
    dao.setup(**kwargs)
