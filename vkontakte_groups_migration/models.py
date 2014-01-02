# -*- coding: utf-8 -*-
from django.db import models
from django.db.models import Q
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

    @opt_generator
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

        offset_step = 1000
        while True:
            response = api_call('groups.getMembers', gid=group.remote_id, offset=offset)
            ids = response['users']
            log.debug('Call returned %s ids for group "%s" with offset %s, now members_ids %s' % (len(ids), group, offset, len(stat.members_ids)))

            if len(ids) == 0:
                break

            # add new ids to group stat members
            stat.members_ids += ids
            stat.offset = offset
#            stat.save()
            offset += offset_step
            yield (offset+len(ids), response['count'], offset_step)

        # save stat with time and other fields
        stat.time = datetime.now()
        stat.save_final()
        signals.group_migration_updated.send(sender=GroupMigration, instance=stat)

class GroupMigration(models.Model):
    class Meta:
        verbose_name = u'Миграция пользователей группы Вконтакте'
        verbose_name_plural = u'Миграции пользователей групп Вконтакте'
        unique_together = ('group','time')
        ordering = ('group','time','-id')

    class QuerySet(QuerySet, GroupMigrationQueryset):
        pass

    group = models.ForeignKey(Group, verbose_name=u'Группа', related_name='migrations')
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

    @property
    def user_ids(self):
        return GroupMembership.objects.filter(group=self.group) \
            .filter(Q(time_entered=None, time_left=None) | \
                    Q(time_entered=None, time_left__gt=self.time) | \
                    Q(time_entered__lte=self.time, time_left=None)).values_list('user_id', flat=True)

    @property
    def entered_user_ids(self):
        return GroupMembership.objects.filter(group=self.group).filter(time_entered=self.time).values_list('user_id', flat=True)

    @property
    def left_user_ids(self):
        return GroupMembership.objects.filter(group=self.group).filter(time_left=self.time).values_list('user_id', flat=True)

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

    def fix_memberships(self):

        if self.next:
            # delete memberships stopped in current and started from the next migration, we need to join them
            GroupMembership.objects.filter(group=self.group, user_id__in=self.members_left_ids,
                time_left=None, time_entered=self.next.time).delete()

            # move left users to the time of the next migration
            GroupMembership.objects.filter(group=self.group, user_id__in=self.members_left_ids, time_left=self.time) \
                .exclude(user_id__in=self.next.members_ids) \
                .update(time_left=self.next.time)

            # these users not entered actually -> delete them
            GroupMembership.objects.filter(group=self.group, user_id__in=self.members_entered_ids, time_entered=self.time) \
                .exclude(user_id__in=self.next.members_ids).delete()

            # update entered_time of all entered users to the time of next migration
            GroupMembership.objects.filter(group=self.group, user_id__in=self.members_entered_ids, time_entered=self.time) \
                .filter(user_id__in=self.next.members_ids).update(time_entered=self.next.time)
        else:
            # if no next migration -> delete all current entered users
            GroupMembership.objects.filter(group=self.group, user_id__in=self.members_entered_ids, time_entered=self.time).delete()

        # update rest of left users -> they are not left
        GroupMembership.objects.filter(group=self.group, user_id__in=self.members_left_ids, time_left=self.time).update(time_left=None)

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
            self.fix_memberships()
            self.update_next()

    def save_final(self):
        '''
        Update local fields, update memberships models and save model,
        Recommended to use in the last moment of creating migration
        '''
        self.offset = 0
        self.clean_members()
        self.update()
        self.update_users_memberships()
        self.save()

    def compare_with_previous(self):
        if self.hidden or not self.prev:
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
            assert not self.hidden
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

    def clean_members(self):
        '''
        Remove double and empty values
        '''
        self.members_ids = list(set(self.members_ids))

    def update(self):
        self.update_left_entered()
        self.update_deactivated()
        self.update_with_avatar()
        self.update_counters()

    def update_left_entered(self):
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

    def update_users_relations(self):
        '''
        Fetch all users of group, make new m2m relations, remove old m2m relations
        '''
        log.debug('Fetching users for the group "%s"' % self.group)
        User.remote.fetch(ids=self.user_ids, only_expired=True)

        # process entered nad left users of the group
        # here is possible using relative self.members_*_ids, but it's better absolute values, calculated by self.group.users
        ids_current = self.group.users.values_list('remote_id', flat=True)
        ids_left = set(ids_current).difference(set(self.members_ids))
        ids_entered = set(self.members_ids).difference(set(ids_current))

        log.debug('Adding %d new users to the group "%s"' % (len(ids_entered), self.group))
        ids = User.objects.filter(remote_id__in=ids_entered).values_list('pk', flat=True)
        self.group.users.through.objects.bulk_create([self.group.users.through(group_id=self.group.pk, user_id=id) for id in ids])

        log.info('Removing %d left users from the group "%s"' % (len(ids_left), self.group))
        ids = User.objects.filter(remote_id__in=ids_left).values_list('pk', flat=True)
        self.group.users.through.objects.filter(group_id=self.group.pk, user_id__in=ids).delete()

        signals.group_users_updated.send(sender=Group, instance=self.group)
        log.info('Updating m2m relations of users for group "%s" successfuly finished' % self.group)
        return True

    def update_users_memberships(self):
        '''
        Fetch all users of group, make new m2m relations, remove old m2m relations
        '''
        if not self.prev:
            # it's first migration -> create memberships
            GroupMembership.objects.bulk_create([GroupMembership(group=self.group, user_id=user_id) for user_id in self.members_ids])
        else:
            memberships_count = GroupMembership.objects.filter(group=self.group, time_left=None).count()
            if self.prev.members_count != memberships_count:
                # something wrong with previous migration
                raise Exception("Number of current memberships %d is not equal to members count %d of previous migration, group %s at %s" % (memberships_count, self.prev.members_count, self.group, self.time))

        # ensure entered users not in memberships now
        error_ids_count = GroupMembership.objects.filter(group=self.group, time_left=None, user_id__in=self.members_entered_ids).count()
        if error_ids_count != 0:
            raise Exception("Found %d just enteted users, that still not left from the group %s at %s" % (error_ids_count, self.group, self.time))
#            GroupMembership.objects.filter(group=self.group, time_left=None, user_id__in=self.members_entered_ids).update(time_left=self.time)

        # create entered users
        GroupMembership.objects.bulk_create([GroupMembership(group=self.group, user_id=user_id, time_entered=self.time) for user_id in self.members_entered_ids])

        # update left users
        GroupMembership.objects.filter(group=self.group, time_left=None, user_id__in=self.members_left_ids).update(time_left=self.time)

        return True

class GroupMembershipManager(models.Manager):

    def get_user_ids_of_period(self, group, date_from, date_to, field=None):

        if field is None:
            kwargs = {'time_entered': None, 'time_left': None} \
                | {'time_entered__lte': date_from, 'time_left': None} \
                | {'time_entered': None, 'time_left__gte': date_to}
        elif field in ['left','entered']:
            kwargs = {'time_%s__gt' % field: date_from, 'time_%s__lte' % field: date_to}
        else:
            raise ValueError("Attribute `field` should be equal to 'left' of 'entered'")

        return self.filter(group=group, **kwargs).order_by('user_id').distinct('user_id').values_list('user_id', flat=True)

    def get_active_users_ids_of_date(self, group, date):
        return self.filter(group=group, **kwargs).order_by('user_id').distinct('user_id').values_list('user_id', flat=True)

class GroupMembership(models.Model):
    class Meta:
        verbose_name = u'Членство пользователя группы Вконтакте'
        verbose_name_plural = u'Членства пользователей групп Вконтакте'
        unique_together = (('group','user_id','time_entered'), ('group','user_id','time_left'),)
        ordering = ('group', 'user_id', 'id')

    group = models.ForeignKey(Group, verbose_name=u'Группа', related_name='memberships')
    user_id = models.PositiveIntegerField(u'ID пользователя', db_index=True)

    time_entered = models.DateTimeField(u'Дата и время вступления', null=True, db_index=True)
    time_left = models.DateTimeField(u'Дата и время выхода', null=True, db_index=True)

    objects = GroupMembershipManager()

import signals
