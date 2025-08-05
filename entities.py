from feast import Entity, ValueType, Project

project = Project(name="homecredit")

application_entity = Entity(
    name="application", join_keys=["SK_ID_CURR"], value_type=ValueType.STRING
)