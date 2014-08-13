import datetime

from sqlalchemy.engine import create_engine
from sqlalchemy import orm, schema, types


def now():
    return datetime.datetime.now()

class Storage:
    """ Class that helps with storage management.
    """

    class File(object):
        """ Object that represents file.
        """

    class Import(object):
        """ Object that represents file import.
        """

    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
        self._define_tables()
        self._map_tables()
        self._create_session()

    def _define_tables(self):
        """ Creates table objects.
        """
        self.metadata = schema.MetaData(self.engine)
        self.file_table = schema.Table(
                'file',
                self.metadata,
                schema.Column('id', types.Integer, primary_key=True),
                schema.Column('sha1', types.String(40), unique=True),
                schema.Column('perceptive_hash', types.String(40), unique=False),
                schema.Column('mime_type', types.String(64), index=True),
                schema.Column('meta', types.Text()),
                schema.Column(
                    'timestamp',
                    types.TIMESTAMP(),
                    default=now(),
                    index=True),
                )
        self.import_table = schema.Table(
                'import',
                self.metadata,
                schema.Column('id', types.Integer, primary_key=True),
                schema.Column('dest', types.String(256), index=True),
                schema.Column('source', types.String(256), index=True),
                schema.Column(
                    'file_id',
                    types.Integer,
                    schema.ForeignKey('file.id')),
                schema.Column(
                    'timestamp',
                    types.TIMESTAMP(),
                    default=now(),
                    index=True),
                )
        self.metadata.create_all()

    def _map_tables(self):
        """ Maps tables to objects.
        """
        orm.mapper(
            Storage.File,
            self.file_table,
            properties={
                'imports': orm.relation(Storage.Import, backref='file'),
                })
        orm.mapper(Storage.Import, self.import_table)

    def _create_session(self):
        """ Creates session
        """
        sm = orm.sessionmaker(
            bind=self.engine,
            autoflush=True,
            autocommit=False,
            expire_on_commit=True,
            )
        self.session = orm.scoped_session(sm)

    def commit(self):
        """ Commits changes.
        """
        self.session.flush()
        self.session.commit()

    def create_file(self, sha1, perceptive_hash, mime_type, meta):
        """ Creates file record.
        """
        query = self.session.query(Storage.File)
        results = query.filter(Storage.File.sha1 == sha1).all()
        if results:
            for result in results:
                print(u'File ID:{}'.format(result.id))
                for import_record in result.imports:
                    print(u'  Dest: {} '.format(import_record.dest))
                    print(u'  Source: {} '.format(import_record.source))
            raise Exception(u'Duplicate found: {}'.format(sha1))
        else:
            record = Storage.File()
            record.sha1 = sha1
            record.perceptive_hash = perceptive_hash
            record.mime_type = mime_type
            record.meta = meta
            self.session.add(record)
            return record

    def create_import(self, file_record, dest, source):
        """ Creates import record.
        """
        record = Storage.Import()
        record.dest = dest
        record.source = source
        record.file = file_record
        self.session.add(record)
        return record

    def already_checked_import(self, source):
        """ Checks if source was already analysed for import.
        """
        query = self.session.query(Storage.Import)
        count = query.filter(Storage.Import.source == source).count()
        return count > 0
