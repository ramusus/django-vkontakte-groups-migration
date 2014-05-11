Django Vkontakte Groups Migration
=================================

[![PyPI version](https://badge.fury.io/py/django-vkontakte-groups-migration.png)](http://badge.fury.io/py/django-vkontakte-groups-migration) [![Build Status](https://travis-ci.org/ramusus/django-vkontakte-groups-migration.png?branch=master)](https://travis-ci.org/ramusus/django-vkontakte-groups-migration) [![Coverage Status](https://coveralls.io/repos/ramusus/django-vkontakte-groups-migration/badge.png?branch=master)](https://coveralls.io/r/ramusus/django-vkontakte-groups-migration)

Приложение позволяет взаимодействовать с историей миграции пользователей в группах Вконтакте через Вконтакте API используя стандартные модели Django

Установка
---------

    pip install django-vkontakte-groups-migration

В `settings.py` необходимо добавить:

    INSTALLED_APPS = (
        ...
        'oauth_tokens',
        'taggit',
        'vkontakte_api',
        'vkontakte_places',
        'vkontakte_users',
        'vkontakte_groups',
        'vkontakte_groups_migration',
    )

    # oauth-tokens settings
    OAUTH_TOKENS_HISTORY = True                                         # to keep in DB expired access tokens
    OAUTH_TOKENS_VKONTAKTE_CLIENT_ID = ''                               # application ID
    OAUTH_TOKENS_VKONTAKTE_CLIENT_SECRET = ''                           # application secret key
    OAUTH_TOKENS_VKONTAKTE_SCOPE = ['ads,wall,photos,friends,stats']    # application scopes
    OAUTH_TOKENS_VKONTAKTE_USERNAME = ''                                # user login
    OAUTH_TOKENS_VKONTAKTE_PASSWORD = ''                                # user password
    OAUTH_TOKENS_VKONTAKTE_PHONE_END = ''                               # last 4 digits of user mobile phone

Покрытие методов API
--------------------

* [groups.getMembers](http://vk.com/dev/groups.getMembers) – возвращает список участников группы;

В планах:

* Перенести реализацию groups.getMembers в приложение [`django-vkontakte-groups`](http://github.com/ramusus/django-vkontakte-groups/);

Примеры использования
---------------------

### Получение среза подписчиков группы

    >>> from vkontakte_groups.models import Group
    >>> group = Group.remote.fetch(ids=[16297716])[0]
    >>> group.update_users()

Срез подписчиков доступен через менеджер

    >>> migration = group.migrations.all()[0]
    >>> len(migration.members_ids)
    5277888
    >>> migration.members_count
    5277888

Подписчики доступны через менеджер

    >>> group.users.count()
    5277888