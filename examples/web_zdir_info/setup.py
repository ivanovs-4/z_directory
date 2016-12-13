from setuptools import setup

setup(
    name='web-zdir-info-serve',
    version='1.0',
    py_modules=['app'],
    include_package_data=True,
    install_requires=[
        'bottle',
        'pyzmq',
        'uwsgi',
    ],
    entry_points='''
        [console_scripts]
        web_zdir_info_dev=app:main
    ''',
)
