from decouple import config
from subprocess import call
from os import path, environ
from datetime import datetime, date


class BackupError(Exception):
    def __init__(self, msg=''):
        self.msg = msg
        logger = Logger()
        logger.exception(self)


class Logger(object):
    def __init__(self):
        self.log_file = config('LOG_FILE', False)

    def log(self, message):
        now = datetime.now()
        line = '{0:%Y-%m-%d %H:%M:%S}: {1}'.format(now, message)
        if self.log_file:
            try:
                with open(self.log_file, 'a') as f:
                    f.write(line)
                    f.write('\n')
            except IOError:
                self.log_file = False
        print(line)

    def exception(self, exception):
        message = '%s: %s' % (exception.__class__.__name__, exception.msg)
        self.log(message)


class BackupManager(object):
    def __init__(self):
        self.logger = Logger()
        self.local_destination = config('LOCAL_DESTINATION')
        self.destination = config('BACKUP_DESTINATION')
        self.filename = self.get_filename()

    def _set_environ(self):
        # pg_dumpall needs this environment variables
        variables = ['PGHOST', 'PGUSER', 'PGPASSWORD']
        for var in variables:
            if not environ.get(var):
                environ[var] = config(var)

    def get_filename(self):
        filename = '{0}.sql'.format(date.today())
        return path.join(self.local_destination, filename)

    def store_file(self):
        if path.isfile(self.filename):
            args = ['aws', 's3', 'cp', self.filename, self.destination]
            return_code = call(args)
            if return_code != 0:
                raise BackupError('aws exit with code %d' % return_code)
        else:
            raise BackupError('File %s not found' % self.filename)

    def execute(self):
        self.logger.log('Iniciando processo')
        self.logger.log('Gerando backup postgres: %s' % self.filename)
        args = ['pg_dumpall', '-f', self.filename]
        self._set_environ()
        return_code = call(args)
        if return_code != 0:
            raise BackupError('pg_dumpall exit with code %d' % return_code)
        self.store_file()
        self.logger.log('OK')


if __name__ == '__main__':
    backup = BackupManager()
    backup.execute()

