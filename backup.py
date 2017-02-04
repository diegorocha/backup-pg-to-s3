from decouple import config
from subprocess import call
from os import path, environ
from datetime import datetime, date


class BackupError(Exception):
    pass


def set_environ():
    # pg_dumpall needs this environment variables
    variables = ['PGHOST', 'PGUSER', 'PGPASSWORD']
    for var in variables:
        if not environ.get(var):
            environ[var] = config(var)


def format_date(date):
    return date.strftime("")


def log(mensagem):
    now = datetime.now()
    print('{0:%Y-%m-%d %H:%M:%S}: {1}'.format(now, mensagem))


def get_filename():
    local_destination = config('LOCAL_DESTINATION')
    filename = '{0}.sql'.format(date.today())
    return path.join(local_destination, filename)


def store_file(filename):
    if path.isfile(filename):
        destination = config('BACKUP_DESTINATION')
        args = ['aws', 's3', 'cp', filename, destination]
        return_code = call(args)
        if return_code != 0:
            raise BackupError('aws exit with code %d' % return_code)
    else:
        raise BackupError('File %s not found' % filename)


def execute_backup():
    filename = get_filename()
    log('Iniciando processo')
    log('Gerando backup postgres: %s' % filename)
    args = ['pg_dumpall',  '-f', filename]
    set_environ()
    return_code = call(args)
    if return_code != 0:
        raise BackupError('pg_dumpall exit with code %d' % return_code)
    store_file(filename)
    log('OK')


if __name__ == '__main__':
    execute_backup()

