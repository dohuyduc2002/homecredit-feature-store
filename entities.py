from feast import Entity, ValueType, Project

project = Project(name="homecredit")

# Spark entities
application_entity = Entity(
    name="application", join_keys=["SK_ID_CURR"], value_type=ValueType.STRING
)

previous_application_entity = Entity(
    name="previous-application", join_keys=["SK_ID_CURR"], value_type=ValueType.STRING
)

# Kafka entities
bureau_balance_entity = Entity(
    name="bureau-balance", join_keys=["SK_ID_CURR"], value_type=ValueType.STRING
)

installments_payments_entity = Entity(
    name="installments-payments", join_keys=["SK_ID_CURR"], value_type=ValueType.STRING
)

credit_card_balance_entity = Entity(
    name="credit-card-balance", join_keys=["SK_ID_CURR"], value_type=ValueType.STRING
)

bureau_entity = Entity(
    name="bureau", join_keys=["SK_ID_CURR"], value_type=ValueType.STRING
)

pos_cash_balance_entity = Entity(
    name="pos-cash-balance", join_keys=["SK_ID_CURR"], value_type=ValueType.STRING
)