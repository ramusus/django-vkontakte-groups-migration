# -*- coding: utf-8 -*-
from celery.task import Task
from vkontakte_groups_migration.models import GroupMigration, update_group_users

class VkontakteGroupUpdateUsersM2M(Task):

    def run(self, stat_id, **kwargs):
        stat = GroupMigration.objects.get(pk=stat_id)
        logger = self.get_logger(**kwargs)
        logger.info(u'VK group "%s" users m2m relations updating started' % stat.group)
        update_group_users(stat.group)
        logger.info(u'VK group "%s" users m2m relations succesfully updated' % stat.group)
        return True