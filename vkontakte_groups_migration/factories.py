from vkontakte_groups.factories import GroupFactory
from models import GroupMigration
import factory

class GroupMigrationFactory(factory.Factory):
    FACTORY_FOR = GroupMigration

    group = factory.SubFactory(GroupFactory)