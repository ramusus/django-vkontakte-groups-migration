# -*- coding: utf-8 -*-
from celery.task import Task
from vkontakte_groups_migration.models import GroupMigration

class VkontakteGroupUpdateUsersM2M(Task):

    def run(self, stat_id, **kwargs):
        stat = GroupMigration.objects.get(pk=stat_id)
        logger = self.get_logger(**kwargs)
        logger.info(u'VK group "%s" users m2m relations updating started' % stat.group)
        try:
            stat.update_users_relations()
            logger.info(u'VK group "%s" users m2m relations succesfully updated' % stat.group)
        except:
            logger.error(u'Unknown error while updating users m2m relations of VK group "%s"' % stat.group)
        return True