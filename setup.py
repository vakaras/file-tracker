#!/usr/bin/python
# -*- coding: utf-8 -*-

from distutils.core import setup


setup(
        name=u'file-tracker'.encode('utf-8'),
        version='0.1',
        author=u'Vytautas Astrauskas'.encode('utf-8'),
        author_email=u'vastrauskas@gmail.com'.encode('utf-8'),
        packages=['file_tracker',],
        package_dir={'': 'src'},
        #package_data={'file_tracker': []},
                                        # List of data files to be included 
                                        # into package.
        requires=[
            'distribute',
            ],
        install_requires=[              # Dependencies for the package.
            'hachoir-metadata',
            'hachoir-parser',
            'hachoir-core',
            'SQLAlchemy',
            'pHash',
            'SimpleCV',
            ],
        scripts=[],                     # List of python script files.
        entry_points = {'console_scripts': ['file-tracker = file_tracker:main',]},
        #data_files=[('/etc/init.d', ['init-script'])]
                                        # List of files, which have to
                                        # be installed into specific 
                                        # locations.
        #url='',                        # Home page.
        #download_url='',               # Page from which package could
                                        # be downloaded.
        description=u'file-tracker'.encode('utf-8'),
        long_description=(
            open('README.rst').read()+open('CHANGES.txt').read()),
        # Full list of classifiers could be found at:
        # http://pypi.python.org/pypi?%3Aaction=list_classifiers
        classifiers=[
            'Development Status :: 1 - Planning',
            #'Environment :: Console',
            #'Framework :: Django',
            'Intended Audience :: Developers',
            (
                'License :: OSI Approved :: '
                'GNU Library or Lesser General Public License (LGPL)'),
            #'Natural Language :: Lithuanian',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            (
                'Topic :: Software Development :: Libraries :: '
                'Python Modules'),
            ],
        license='LGPL'
        )
