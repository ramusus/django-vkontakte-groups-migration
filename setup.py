from setuptools import setup, find_packages

setup(
    name='django-vkontakte-groups-migration',
    version=__import__('vkontakte_groups_migration').__version__,
    description='Django implementation for vkontakte API Groups Users Migration',
    long_description=open('README.md').read(),
    author='ramusus',
    author_email='ramusus@gmail.com',
    url='https://github.com/ramusus/django-vkontakte-groups-migration',
    download_url='http://pypi.python.org/pypi/django-vkontakte-groups-migration',
    license='BSD',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False, # because we're including media that Django needs
    install_requires=[
        'django-vkontakte-api>=0.4.5',
        'django-vkontakte-groups>=0.3.4',
        'django-vkontakte-users>=0.4.7',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
