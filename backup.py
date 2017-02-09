from boto import connect_s3
from decouple import config
from subprocess import call
from os import path, environ, fstat, SEEK_END
from datetime import datetime, date
from boto.s3.key import Key


class BackupError(Exception):
    pass


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
        message = '%s: %s' % (exception.__class__.__name__, str(exception))
        self.log(message)


class BackupManager(object):
    def __init__(self):
        self.logger = Logger()
        self.local_destination = config('LOCAL_DESTINATION')
        self.destination = config('BACKUP_DESTINATION')

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
        success = False
        if path.isfile(self.filename):
            with open(self.filename) as file:
                aws_access_key_id = config('AWS_ACCESS_KEY_ID')
                aws_secret_access_key = config('AWS_SECRET_ACCESS_KEY')
                bucket_args = list([p for p in config('BACKUP_DESTINATION').split('/') if p])
                bucket = bucket_args[1]
                bucket_args.append(path.split(self.filename)[1])
                key = '/'.join(bucket_args[2:])
                print(bucket, key)
                try:
                    size = fstat(file.fileno()).st_size
                except Exception:
                    # Not all file objects implement fileno(),
                    # so we fall back on this
                    file.seek(0, SEEK_END)
                    size = file.tell()
                conn = connect_s3(aws_access_key_id, aws_secret_access_key)
                bucket = conn.get_bucket(bucket, validate=True)
                k = Key(bucket)
                k.key = key
                # if content_type:
                #     k.set_metadata('Content-Type', content_type)
                sent = k.set_contents_from_file(file, reduced_redundancy=False, rewind=True)
                if sent == size:
                    success = True
        if not success:
            raise BackupError('Cannot send %s to s3' % self.filename)

    def execute(self):
        try:
            self.filename = self.get_filename()
            self.logger.log('Iniciando processo')
            self.logger.log('Gerando backup postgres: %s' % self.filename)
            self._set_environ()
            args = ['pg_dumpall', '-f', self.filename]
            return_code = call(args)
            if return_code != 0:
                raise BackupError('pg_dumpall exit with code %d' % return_code)
            self.logger.log('Enviando para s3')
            self.store_file()
            self.logger.log('OK')
        except Exception as ex:
            self.logger.exception(ex)


if __name__ == '__main__':
    backup = BackupManager()
    backup.execute()
