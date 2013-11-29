# -*- coding: utf-8 -*-
from django.db import models
from django.db.models.query import QuerySet
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from django.conf import settings
from vkontakte_api import fields
from vkontakte_api.utils import api_call
from vkontakte_api.decorators import opt_generator
from vkontakte_groups.models import Group
from vkontakte_users.models import User
from datetime import datetime, timedelta
import logging

log = logging.getLogger('vkontakte_groups_migration')

GROUPS_MIGRATION_USER_TYPE_MEMBER = 1
GROUPS_MIGRATION_USER_TYPE_ENTERED = 2
GROUPS_MIGRATION_USER_TYPE_LEFT = 3
GROUPS_MIGRATION_USER_TYPE_CHOICES = (
    (GROUPS_MIGRATION_USER_TYPE_MEMBER, 'member'),
    (GROUPS_MIGRATION_USER_TYPE_ENTERED, 'entered'),
    (GROUPS_MIGRATION_USER_TYPE_LEFT, 'left'),
)

def ModelQuerySetManager(ManagerBase=models.Manager):
    '''
    Function that return Manager for using QuerySet class inside the model definition
    @param ManagerBase - parent class Manager
    '''
    if not issubclass(ManagerBase, models.Manager):
        raise ValueError("Parent class for ModelQuerySetManager must be models.Manager or it's child")

    class Manager(ManagerBase):
        '''
        Manager based on QuerySet class inside the model definition
        '''
        def get_query_set(self):
            return self.model.QuerySet(self.model)

    return Manager()

class GroupMigrationQueryset(object):

    @property
    def visible(self):
        return self.exclude(hidden=True).exclude(time__isnull=True)

class GroupMigrationManager(models.Manager, GroupMigrationQueryset):

    @opt_generator
    def update_for_group(self, group, offset=0):
        '''
        Fetch all users for this group, save them as IDs and after make m2m relations
        '''
        try:
            migration, created = self.get_or_create(group=group, time=None)
        except MultipleObjectsReturned:
            self.filter(group=group, time=None).delete()
            migration = self.create(group=group, time=None, offset=0)
            created = True

        members_ids = []

        offset_step = 1000
        while True:
            response = api_call('groups.getMembers', gid=group.remote_id, offset=offset)
            ids = response['users']
            log.debug('Call returned %d ids for group "%s" with offset %s, total count %d' % (len(ids), group, offset, len(members_ids)))

            if len(ids) == 0:
                break

            members_ids += ids
            offset += offset_step
            yield (offset + len(ids), response['count'], offset_step)

        log.debug('Add all %d remote ids to group "%s"' % (len(members_ids), group))
        migration.add_members(members_ids)

        log.debug('Final save group migration "%s"' % group)
        migration.save_final()
        signals.group_migration_updated.send(sender=GroupMigration, instance=migration)


class GroupMigrationOld(models.Model):
    class Meta:
        verbose_name = u'Миграция пользователей группы Вконтакте'
        verbose_name_plural = u'Миграции пользователей групп Вконтакте'
        unique_together = ('group','time')
        ordering = ('group','time','-id')

    class QuerySet(QuerySet, GroupMigrationQueryset):
        pass

    group = models.ForeignKey(Group, verbose_name=u'Группа', related_name='migrations_old')
    time = models.DateTimeField(u'Дата и время', null=True, db_index=True)

    hidden = models.BooleanField(u'Скрыть', db_index=True)

    offset = models.PositiveIntegerField(default=0)

    members_ids = fields.PickledObjectField(default=[])
    members_entered_ids = fields.PickledObjectField(default=[])
    members_left_ids = fields.PickledObjectField(default=[])
    members_deactivated_entered_ids = fields.PickledObjectField(default=[])
    members_deactivated_left_ids = fields.PickledObjectField(default=[])
    members_has_avatar_entered_ids = fields.PickledObjectField(default=[])
    members_has_avatar_left_ids = fields.PickledObjectField(default=[])

    members_count = models.PositiveIntegerField(default=0)
    members_entered_count = models.PositiveIntegerField(default=0)
    members_left_count = models.PositiveIntegerField(default=0)
    members_deactivated_entered_count = models.PositiveIntegerField(default=0)
    members_deactivated_left_count = models.PositiveIntegerField(default=0)
    members_has_avatar_entered_count = models.PositiveIntegerField(default=0)
    members_has_avatar_left_count = models.PositiveIntegerField(default=0)

    objects = ModelQuerySetManager(GroupMigrationManager)

    def set_defaults(self):
        '''
        It's neccesary to call after creating of every instance,
        because `default` attribute of fields.PickledObjectField doesn't work properly
        '''
        self.members_ids = []
        self.members_entered_ids = []
        self.members_left_ids = []
        self.members_deactivated_entered_ids = []
        self.members_deactivated_left_ids = []
        self.members_has_avatar_entered_ids = []
        self.members_has_avatar_left_ids = []

    def clean_members(self):
        '''
        Remove double and empty values
        '''
        self.members_ids = list(set(self.members_ids))

    def update(self):
        self.update_migration()
        self.update_deactivated()
        self.update_with_avatar()
        self.update_counters()

    def update_migration(self):
        prev_stat = self.prev
        if self.prev and self.group:
            self.members_left_ids = list(set(prev_stat.members_ids).difference(set(self.members_ids)))
            self.members_entered_ids = list(set(self.members_ids).difference(set(prev_stat.members_ids)))

    def update_deactivated(self):
        self.members_deactivated_entered_ids = list(User.objects.deactivated().filter(remote_id__in=self.members_entered_ids).values_list('remote_id', flat=True))
        self.members_deactivated_left_ids = list(User.objects.deactivated().filter(remote_id__in=self.members_left_ids).values_list('remote_id', flat=True))

    def update_with_avatar(self):
        self.members_has_avatar_entered_ids = list(User.objects.has_avatars().filter(remote_id__in=self.members_entered_ids).values_list('remote_id', flat=True))
        self.members_has_avatar_left_ids = list(User.objects.has_avatars().filter(remote_id__in=self.members_left_ids).values_list('remote_id', flat=True))


class GroupMigration(models.Model):
    class Meta:
        verbose_name = u'Миграция пользователей группы Вконтакте'
        verbose_name_plural = u'Миграции пользователей групп Вконтакте'
        unique_together = ('group','time')
        ordering = ('group','-time','-id')

    class QuerySet(QuerySet, GroupMigrationQueryset):
        pass

    group = models.ForeignKey(Group, verbose_name=u'Группа', related_name='migrations')
    time = models.DateTimeField(u'Дата и время', null=True, db_index=True)

    hidden = models.BooleanField(u'Скрыть', db_index=True)

    offset = models.PositiveIntegerField(default=0)

    members_count = models.PositiveIntegerField(default=0)
    members_entered_count = models.PositiveIntegerField(default=0)
    members_left_count = models.PositiveIntegerField(default=0)
    members_deactivated_entered_count = models.PositiveIntegerField(default=0)
    members_deactivated_left_count = models.PositiveIntegerField(default=0)
    members_has_avatar_entered_count = models.PositiveIntegerField(default=0)
    members_has_avatar_left_count = models.PositiveIntegerField(default=0)

    objects = ModelQuerySetManager(GroupMigrationManager)

    @property
    def members_ids(self):
        return self.users.filter(type=GROUPS_MIGRATION_USER_TYPE_MEMBER).values_list('user_id', flat=True)

    @property
    def members_left_ids(self):
        return self.users.filter(type=GROUPS_MIGRATION_USER_TYPE_LEFT).values_list('user_id', flat=True)

    @property
    def members_entered_ids(self):
        return self.users.filter(type=GROUPS_MIGRATION_USER_TYPE_ENTERED).values_list('user_id', flat=True)

    @property
    def next(self):
        try:
            return self.group.migrations.visible.filter(time__gt=self.time).order_by('time')[0]
        except IndexError:
            return None

    @property
    def prev(self):
        try:
            return self.group.migrations.visible.filter(time__lt=self.time).order_by('-time')[0]
        except IndexError:
            return None

    def add_members(self, ids, type=GROUPS_MIGRATION_USER_TYPE_MEMBER):
        return GroupMigrationUser.objects.bulk_create([GroupMigrationUser(migration=self, user_id=id, type=type) for id in ids])

    def delete(self, *args, **kwargs):
        '''
        Recalculate next stat members instance
        '''
        self.hide()
        super(GroupMigration, self).delete(*args, **kwargs)

    def hide(self):
        '''
        Hide curent migration, and recalculate fields of next migrations
        '''
        self.hidden = True
        self.save()

    def update_next(self):
        next_stat = self.next
        if next_stat:
            next_stat.update()
            next_stat.save()

    def save(self, *args, **kwargs):
        update_next = False
        if self.id and self.hidden != self.__class__.objects.get(id=self.id).hidden:
            update_next = True

        super(GroupMigration, self).save(*args, **kwargs)

        if update_next:
            self.update_next()

    def save_final(self):
        self.time = datetime.now()
        self.offset = 0
        self.update()
#        self.compare_with_statistic()
#        self.compare_with_previous()
        self.save()

    def compare_with_previous(self):
        if not self.prev:
            return

        delta = self.time - self.prev.time

        if delta > timedelta(2):
            return

        division = float(self.members_count) / self.prev.members_count
        if 0.9 < division < 1.1:
            return
        else:
            log.warning("Suspicious migration found. Previous value is %d, current value is %d, time delta %s. Group %s, migration ID %d" % (self.prev.members_count, self.members_count, delta, self.group, self.id))
            self.hide()

    def compare_with_statistic(self):
        try:
            assert 'vkontakte_groups_statistic' in settings.INSTALLED_APPS
            members_count = self.group.statistics.get(date=self.time.date()).members
        except (ObjectDoesNotExist, AssertionError):
            return

        division = float(self.members_count) / members_count
        if 0.99 < division < 1.01:
            return
        else:
            log.warning("Suspicious migration found. Statistic value is %d, API value is %d. Group %s, migration ID %d" % (members_count, self.members_count, self.group, self.id))
            self.hide()

    def update(self):
        self.members_count = self.members_ids.count()

        if self.prev and self.group:

            members_ids_prev = self.prev.members_ids
            members_ids = self.members_ids

            members_left_ids = list(set(members_ids_prev).difference(set(members_ids)))
            members_entered_ids = list(set(members_ids).difference(set(members_ids_prev)))

            self.users.filter(type__in=[GROUPS_MIGRATION_USER_TYPE_LEFT, GROUPS_MIGRATION_USER_TYPE_ENTERED]).delete()
            self.add_members(members_left_ids, GROUPS_MIGRATION_USER_TYPE_LEFT)
            self.add_members(members_entered_ids, GROUPS_MIGRATION_USER_TYPE_ENTERED)

            self.members_left_count = len(members_left_ids)
            self.members_entered_count = len(members_entered_ids)

#             self.members_deactivated_entered_count = User.objects.deactivated().filter(remote_id__in=members_entered_ids).order_by().count()
#             self.members_deactivated_left_count = User.objects.deactivated().filter(remote_id__in=members_left_ids).order_by().count()
#
#             self.members_has_avatar_entered_count = User.objects.has_avatars().filter(remote_id__in=members_entered_ids).order_by().count()
#             self.members_has_avatar_left_count = User.objects.has_avatars().filter(remote_id__in=members_left_ids).order_by().count()

    def update_users_relations(self):
        '''
        Fetch all users of group, make new m2m relations, remove old m2m relations
        '''
        log.debug('Fetching users for the group "%s"' % self.group)
        User.remote.fetch(ids=self.members_ids, only_expired=True)

        # process entered nad left users of the group
        # here is possible using relative self.members_*_ids, but it's better absolute values, calculated by self.group.users
        ids_current = self.group.users.values_list('remote_id', flat=True)
        members_ids = list(self.members_ids)
        ids_left = set(ids_current).difference(set(members_ids))
        ids_entered = set(members_ids).difference(set(ids_current))

        log.debug('Adding %d new users to the group "%s"' % (len(ids_entered), self.group))
        for remote_id in ids_entered:
            self.group.users.add(User.objects.get(remote_id=remote_id))

        log.info('Removing %d left users from the group "%s"' % (len(ids_left), self.group))
        for remote_id in ids_left:
            self.group.users.remove(User.objects.get(remote_id=remote_id))

        signals.group_users_updated.send(sender=Group, instance=self.group)
        log.info('Updating m2m relations of users for group "%s" successfuly finished' % self.group)
        return True

class GroupMigrationUser(models.Model):
    class Meta:
        unique_together = ('migration', 'user_id', 'type')

#    commented because of https://code.djangoproject.com/ticket/14286, changed to bigint by the hand
#    id = models.BigAutoField(primary_key=True)
    migration = models.ForeignKey(GroupMigration, verbose_name=u'Пользователь миграции', related_name='users')
    user_id = models.PositiveIntegerField()
    type = models.PositiveIntegerField(choices=GROUPS_MIGRATION_USER_TYPE_CHOICES, db_index=True, default=GROUPS_MIGRATION_USER_TYPE_MEMBER)


import signals

# i = 0
# while True:
#     migrations = GroupMigration.objects.all().order_by('time')[i*100:(i+1)*100]
#     if migrations.count() == 0:
#         break
#     for stat in migrations:
#         print stat.group, stat.time
#         migration = GroupMigrationNew()
#         migration.__dict__.update(stat.__dict__)
#         migration.save()
#         users = []
#         for remote_id in stat.members_ids:
#                     users += [GroupMigrationUser(migration=migration, user_id=remote_id)]
#         GroupMigrationUser.objects.bulk_create(users)
#         print '%d users' % len(users)
# i = i + 1