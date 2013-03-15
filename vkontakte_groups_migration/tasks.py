# -*- coding: utf-8 -*-
from celery.task import Task
from vkontakte_groups_migration.models import GroupMigration

class VkontakteGroupUpdateUsersM2M(Task):

    def run(self, group, **kwargs):
        logger = self.get_logger(**kwargs)
        logger.info(u'VK group "%s" users m2m relations updating started' % group)
        try:
            GroupMigration.objects.update_group_users_m2m(group)
            logger.info(u'VK group "%s" users m2m relations succesfully updated' % group)
        except:
            logger.error(u'Unknown error while updating users m2m relations of VK group "%s"' % group)