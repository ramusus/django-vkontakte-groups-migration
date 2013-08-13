# -*- coding: utf-8 -*-
from django.db import models
from django.db.models.query import QuerySet
from django.core.exceptions import MultipleObjectsReturned
from vkontakte_api import fields
from vkontakte_api.utils import api_call
from vkontakte_groups.models import Group
from vkontakte_users.models import User
from datetime import datetime
import logging

log = logging.getLogger('vkontakte_groups_migration')

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

    @property
    def light(self):
        return self.defer(
            'members_ids',
            'members_entered_ids',
            'members_left_ids',
            'members_deactivated_entered_ids',
            'members_deactivated_left_ids',
            'members_has_avatar_entered_ids',
            'members_has_avatar_left_ids'
        )

class GroupMigrationManager(models.Manager, GroupMigrationQueryset):

    def update_for_group(self, group, offset=0):
        '''
        Fetch all users for this group, save them as IDs and after make m2m relations
        '''
        try:
            stat, created = self.get_or_create(group=group, time=None)
        except MultipleObjectsReturned:
            self.filter(group=group, time=None).delete()
            stat = self.create(group=group, time=None, offset=0)
            created = True

        if created:
            stat.set_defaults()

        offset = offset or stat.offset

        while True:
            response = api_call('groups.getMembers', gid=group.remote_id, offset=offset)
            ids = response['users']
            log.debug('Call returned %s ids for group "%s" with offset %s, now members_ids %s' % (len(ids), group.screen_name, offset, len(stat.members_ids)))

            if len(ids) == 0:
                break

            # add new ids to group stat members
            stat.members_ids += ids
            stat.offset = offset
#            stat.save()
            offset += 1000

        # save stat with time and other fields
        stat.save_final()
        signals.group_migration_updated.send(sender=GroupMigration, instance=stat)

    def update_group_users_m2m(self, stat, offset=0):
        '''
        Fetch all users of group, make new m2m relations, remove old m2m relations
        '''
        group = stat.group

        ids = stat.members_ids
        ids_left = set(group.users.values_list('remote_id', flat=True)).difference(set(ids))

        offset = offset or stat.offset
        errors = 0

        while True:
            ids_sliced = ids[offset:offset+1000]
            if len(ids_sliced) == 0:
                break

            log.debug('Fetching users for group "%s", offset %d' % (group, offset))
            try:
                users = User.remote.fetch(ids=ids_sliced, only_expired=True)
            except Exception, e:
                log.error('Error %s while getting users for group "%s": "%s", offset %d' % (e.__class__, group, e, offset))
                errors += 1
                if errors == 10:
                    log.error('Fail - number of unhandled errors while updating users for group "%s" more than 10, offset %d' % (group, offset))
                    break
                continue

            if len(users) == 0:
                stat.offset = 0
                stat.save()
                break
            else:
                for user in users:
                    if user.id:
                        group.users.add(user)
                stat.offset = offset
                stat.save()
                offset += 1000

        # process left users of group
        log.info('Removing %s left users for group "%s"' % (len(ids_left), group))
        for remote_id in ids_left:
            group.users.remove(User.objects.get(remote_id=remote_id))

        signals.group_users_updated.send(sender=Group, instance=group)
        log.info('Updating m2m relations of users for group "%s" successfuly finished' % (group,))
        return True

class GroupMigration(models.Model):
    class Meta:
        verbose_name = u'Миграция пользователей группы Вконтакте'
        verbose_name_plural = u'Миграции пользователей групп Вконтакте'
        unique_together = ('group','time')
        ordering = ('group','time','-id')

    class QuerySet(QuerySet, GroupMigrationQueryset):
        pass

    group = models.ForeignKey(Group, verbose_name=u'Группа', related_name='migrations')
    time = models.DateTimeField(u'Дата и время', null=True)

    hidden = models.BooleanField(u'Скрыть')

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
        if self.id and self.hidden != self.__class__.objects.light.get(id=self.id).hidden:
            update_next = True

        super(GroupMigration, self).save(*args, **kwargs)

        if update_next:
            self.update_next()

    def save_final(self):
        self.time = datetime.now()
        self.offset = 0
        self.clean_members()
        self.update()
        self.save()

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

    def update_counters(self):
        for field_name in ['members','members_entered','members_left','members_deactivated_entered','members_deactivated_left','members_has_avatar_entered','members_has_avatar_left']:
            setattr(self, field_name + '_count', len(getattr(self, field_name + '_ids')))

import signals