from vkontakte_groups.factories import GroupFactory
from models import GroupMigration
import factory

class GroupMigrationFactory(factory.DjangoModelFactory):
    FACTORY_FOR = GroupMigration

    group = factory.SubFactory(GroupFactory)