#!/usr/bin/python

import datetime
import os
from os import path
import hashlib
import json
import pHash
import shutil
from hachoir_core.cmd_line import unicodeFilename
from hachoir_parser import createParser
from hachoir_metadata import extractMetadata

from file_tracker.models import Storage

class Walker(object):
    """ Walks through all files in root directory.
    """

    def __init__(self, root_src, root_dst, storage, log_file):
        self.root_src = root_src
        self.root_dst = root_dst
        self.storage = storage
        self.log_file = log_file
        self.count = 0

    def __call__(self):
        self.log(u"START", self.root_src)
        self.walk(self.root_src)

    def log(self, action, *args):
        """ Logs event.
        """
        fields = (
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                action,
                ) + args
        result = u"\u263a".join(unicode(field) for field in fields)
        self.log_file.write((result + "\n").encode('utf-8'))
        self.log_file.flush()

    def walk(self, current_path):
        """ Calls callback for each file in path.
        """
        if self.count > 30000:
            return
        else:
            self.count += 1
        if path.isdir(current_path):
            for file_name in sorted(os.listdir(current_path)):
                file_path = path.join(current_path, file_name)
                self.walk(file_path)
        else:
            self.check(current_path)

    def get_sha1(self, file_path):
        """ Calculate the sha1 for the file.
        """
        sha1 = hashlib.sha1()
        with open(file_path, 'rb') as f:
            while True:
                buf = f.read(0x1000000)
                if not buf:
                    break
                sha1.update(buf)
        return sha1.hexdigest()

    def get_perceptive_hash(self, file_path):
        """ Calculate the perceptive hash for the file.
        """
        image_hash = pHash.imagehash(file_path)
        return "%x" % (int(image_hash))

    IMAGE_EXTENSIONS = ('.png', '.jpg', '.jpeg', '.gif', '.nef', '.pef',)
    VIDEO_EXTENSIONS = ('.mov', '.avi', '.wmv', '.mpg', '.mpeg',
                        '.bup', '.ifo', '.vob', '.mp4',)
    AUDIO_EXTENSIONS = ('.mp3', '.wav', '.wma', '.ogg',)
    OTHER_EXTENSIONS = ('.pps', '.ppt', '.pptx', '.odt', '.iso', '.txt',)
    ALL_EXTENSIONS = (
            IMAGE_EXTENSIONS +
            VIDEO_EXTENSIONS +
            AUDIO_EXTENSIONS +
            OTHER_EXTENSIONS)

    def is_image(self, file_path):
        """ Checks extension.
        """
        name, ext = path.splitext(file_path.lower())
        return ext in self.IMAGE_EXTENSIONS

    def check_extension(self, file_path):
        """ If extension is not supported, then raise exception.
        """
        name, ext = path.splitext(file_path.lower())
        if ext not in self.ALL_EXTENSIONS:
            raise Exception("Unknown file extension " + ext)
        return ext in self.IMAGE_EXTENSIONS

    def get_meta(self, file_path):
        """ Get the meta information.
        """
        self.check_extension(file_path)
        filename, realname = unicodeFilename(file_path), file_path
        parser = createParser(filename, realname)
        if parser is None:
            if file_path.lower().endswith('.mov'):
                return 'video/quicktime', 'null'
            if file_path.lower().endswith('.mpg'):
                return 'video/mpeg', 'null'
            if file_path.lower().endswith('.jpg'):
                return 'image/jpeg', 'null'
            if file_path.lower().endswith('.bup'):
                return 'video/dvd', 'null'
            if file_path.lower().endswith('.vob'):
                return 'video/dvd', 'null'
            if file_path.lower().endswith('.ifo'):
                return 'video/dvd', 'null'
        metadata = extractMetadata(parser)
        mime_type = parser.mime_type
        info = {}
        for data in sorted(metadata or ()):
            if not data.values:
                continue
            info[data.key] = [item.text for item in data.values]
        return mime_type, json.dumps(info)

    def get_relative_path(self, file_path):
        """ Returns the destination path.
        """
        return file_path[1+len(self.root_src):]

    def copy(self, dest, src):
        """ Copy file to dest.
        """
        dest_dir = path.dirname(dest)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        shutil.copyfile(src, dest)

    def check(self, file_path):
        """ Checks the file path.
        """
        unicode_file_path = file_path.decode('utf-8')
        small_path = unicode_file_path.lower()
        if self.storage.already_checked_import(unicode_file_path) or \
           small_path.endswith('thumbs.db'):
            self.log(u"IGNORE", unicode_file_path)
        else:
            self.log(u"ANALYSIS", unicode_file_path)
            mime_type, meta = self.get_meta(file_path)
            if self.is_image(file_path):
                perceptive_hash = self.get_perceptive_hash(file_path)
            else:
                perceptive_hash = '';
            sha1 = self.get_sha1(file_path)
            file_record = self.storage.create_file(
                    sha1,
                    perceptive_hash,
                    mime_type,
                    meta,
                    )
            relative_path = self.get_relative_path(unicode_file_path)
            dest_path = os.path.join(self.root_dst, relative_path)
            import_record = self.storage.create_import(
                    file_record,
                    dest_path,
                    unicode_file_path,
                    )
            self.copy(dest_path, file_path)
            self.storage.commit()


def main():
    storage = Storage('sqlite:///tracker.db')
    root_src = "/data/NMA/Nuotraukos"
    root_dst = "/media/data/NMA/Nuotraukos"
    with open('log.csv', 'a') as log:
        walker = Walker(root_src, root_dst, storage, log);
        walker()
