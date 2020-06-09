from faker import Faker as F

leo_eid_format = 'z/%Y/%m/%d/%H/%M/%s/%f-%f'


class FactoryRegistry():
    registry = {}

    icentris_clients = [
        {
            'icentris_client': 'bluesun',
            'partition_id': 4,
            'wrench_id': '5396d067-9e31-4572-951a-a7d1b0a5eaf6',
            'pyr_rank_definitions': [
                {'id_': 1, 'level': 10, 'client_level': '1', 'name': 'Level 1'},
                {'id_': 2, 'level': 20, 'client_level': '2', 'name': 'Level 2'},
                {'id_': 3, 'level': 30, 'client_level': '3', 'name': 'Level 3'}],
            'tree_user_statuses': [
                {'id_': 1, 'description': 'Active'},
                {'id_': 2, 'description': 'Inactive'}],
            'tree_user_types': [
                {'id_': 1, 'description': 'Customer'},
                {'id_': 2, 'description': 'Autoship'},
                {'id_': 3, 'description': 'Distributor'}],
            'tree_order_statuses': [
                {"id_": 1, "description": "Active"}],
            'tree_order_types': [
                {"id_": 1, "description": "Customer"}]},
        {
            'icentris_client': 'worldventures',
            'partition_id': 2,
            'wrench_id': 'd7d3e26f-d105-4816-825d-d5858b9cf0d1',
            'pyr_rank_definitions': [
                {"id_": 1, "level": 10, "client_level": "1", "name": "Enrolled Representative"},
                {"id_": 2, "level": 50, "client_level": "5", "name": "Active Representative"},
                {"id_": 3, "level": 200, "client_level": "20", "name": "Qualified Representative"},
                {"id_": 4, "level": 250, "client_level": "25", "name": "Senior Representative"},
                {"id_": 5, "level": 350, "client_level": "35", "name": "Director"},
                {"id_": 6, "level": 400, "client_level": "40", "name": "Marketing Director"},
                {"id_": 7, "level": 450, "client_level": "45", "name": "Regional Marketing Director"},
                {"id_": 8, "level": 500, "client_level": "50", "name": "National Marketing Director"},
                {"id_": 9, "level": 550, "client_level": "55", "name": "International Marketing Director"},
                {"id_": 16, "level": 0, "client_level": "0", "name": "Pending"},
                {"id_": 17, "level": 210, "client_level": "21", "name": "One Star Representative"},
                {"id_": 18, "level": 220, "client_level": "22", "name": "Two Star Representative"},
                {"id_": 19, "level": 230, "client_level": "23", "name": "Three Star Representative"}],
            'tree_user_statuses': [
                {"id_": 0, "description": "Deleted"},
                {"id_": 1, "description": "Active"},
                {"id_": 2, "description": "Grace"},
                {"id_": 3, "description": "Inactive"},
                {"id_": 4, "description": "Auto Cancelled"},
                {"id_": 5, "description": "Suspended"},
                {"id_": 6, "description": "Terminated"},
                {"id_": 7, "description": "ChargeBack"},
                {"id_": 8, "description": "Resigned"},
                {"id_": 9, "description": "Hold"},
                {"id_": 10, "description": "Pending Grace"}],
            'tree_user_types': [
                {"id_": 1, "description": "Customer"},
                {"id_": 2, "description": "Distributor"},
                {"id_": 3, "description": "B2B"},
                {"id_": 4, "description": "Import"},
                {"id_": 5, "description": "UK-Trial"},
                {"id_": 6, "description": "Employee"},
                {"id_": 7, "description": "RCSCompany"},
                {"id_": 8, "description": "Pending Transfer"},
                {"id_": 9, "description": "MarketPlace Customer"},
                {"id_": 10, "description": "Transfer Complete"},
                {"id_": 11, "description": "Transfer Rejected"},
                {"id_": 12, "description": "Freemium"}],
            'tree_order_statuses': [
                {"id_": 8, "description": "Printed"},
                {"id_": 7, "description": "Accepted"},
                {"id_": 0, "description": "Incomplete"},
                {"id_": 10, "description": "Pending Inventory"},
                {"id_": 1, "description": "Pending"},
                {"id_": 5, "description": "CC Pending"},
                {"id_": 3, "description": "ACH Declined"},
                {"id_": 9, "description": "Shipped"},
                {"id_": 2, "description": "CC Declined"},
                {"id_": 6, "description": "ACH Pending"},
                {"id_": 4, "description": "Cancelled"}],
            'tree_order_types': [
                {"id_": 3, "description": "Web Wizard"},
                {"id_": 7, "description": "Replacement Order"},
                {"id_": 2, "description": "Shopping Cart"},
                {"id_": 4, "description": "Recurring Order"},
                {"id_": 10, "description": "Ticket System"},
                {"id_": 5, "description": "Import"},
                {"id_": 12, "description": "Back Order Parent NoShip"},
                {"id_": 1, "description": "Customer Service"},
                {"id_": 8, "description": "Return Order"},
                {"id_": 13, "description": "Child Order"},
                {"id_": 9, "description": "Web Recurring Order"},
                {"id_": 11, "description": "API Order"},
                {"id_": 6, "description": "Back Order"}]},
        {
            'icentris_client': 'naturessunshine',
            'partition_id': 3,
            'wrench_id': '16bcfb48-153a-4c7d-bb65-19074d9edb17',
            'pyr_rank_definitions': [],
            'tree_user_statuses': [],
            'tree_user_types': [],
            'tree_order_statuses': [],
            'tree_order_types': []},
        {
            'icentris_client': 'monat',
            'partition_id': 1,
            'wrench_id': '2c889143-9169-436a-b610-48c8fe31bb87',
            'pyr_rank_definitions': [],
            'tree_user_statuses': [],
            'tree_user_types': [],
            'tree_order_statuses': [],
            'tree_order_types': []}]

    normalized_types = ['Distributor', 'Customer', 'Retail']

    normalized_statuses = ['Active', 'Inactive']

    @classmethod
    def get_client(cls, name):
        return next(c for c in cls.icentris_clients if c['icentris_client'] == name)

    @classmethod
    def create(cls, factory, **kwargs):
        if factory.__name__ not in cls.registry:
            cls.registry[factory.__name__] = []
        cls.registry[factory.__name__].append(factory(**kwargs))

    @classmethod
    def create_multiple(cls, factory, iters, args=[]):
        for i in range(iters):
            if args[i] is None:
                args[i] = {}
            cls.create(factory, **args[i])

    @classmethod
    def find(cls, factory, icentris_client):
        return [
            f for f in cls.registry[factory.__name__]
            if f['icentris_client'] == icentris_client]

    @classmethod
    def find_by_id(cls, factory, icentris_client, id_):
        return next((
            f for f in cls.registry[factory.__name__]
            if f['id'] == id_
            and f['icentris_client'] == icentris_client),
            None)


class Faker(F):
    def past_datetime_as_str(self):
        return str(fake.past_datetime())

    def leo_eid(self):
        return fake.past_datetime().strftime(leo_eid_format)

    def ingestion_timestamp(self):
        return f'{fake.past_datetime()} UTC'

    def random_relation_id(self, relation, icentris_client):
        ls = FactoryRegistry.find(relation, icentris_client)
        return fake.random_element(ls)['id']

    def random_relation_value(self, relation, field, icentris_client):
        ls = FactoryRegistry.find(relation, icentris_client)
        return fake.random_element(ls)[field]


fake = Faker()


class ModelFactory(dict):
    reserved_attributes = [
        'clear',
        'copy',
        'fromkeys',
        'get',
        'items',
        'keys',
        'pop',
        'popitem',
        'setdefault',
        'update',
        'values',
        'get_client_record',
        'reserved_attributes']

    def __init__(self, *args, **kwargs):
        super(ModelFactory, self).__init__()
        for f in dir(self):
            if not f.startswith('_') and f not in self.reserved_attributes:
                v = None
                if f in kwargs:
                    v = kwargs[f]
                else:
                    a = getattr(self, f)
                    if hasattr(a, '__call__'):
                        v = a()
                    elif isinstance(a, list):
                        n = a.copy()
                        fn = n.pop(0)
                        try:
                            i = n.index('__icentris_client__')
                            n[i] = self.__getitem__('icentris_client')
                        except ValueError:
                            pass

                        v = fn(*n)
                self.__setitem__(f[:-1] if f.endswith('_') else f, v)


class LakeFactory(ModelFactory):
    leo_eid = fake.leo_eid
    ingestion_timestamp = fake.ingestion_timestamp

    def __init__(self, *args, **kwargs):
        if 'icentris_client' in kwargs:
            client = kwargs['icentris_client']
            del kwargs['icentris_client']
        else:
            client = fake.random_element([e['icentris_client'] for e in FactoryRegistry.icentris_clients])
        self.__setitem__('icentris_client', client)
        super(LakeFactory, self).__init__(*args, **kwargs)


class StagingFactory(LakeFactory):
    def __init__(self, *args, **kwargs):
        super(StagingFactory, self).__init__(*args, **kwargs)
        client = FactoryRegistry.get_client(self.__getitem__('icentris_client'))
        self.__setitem__('client_partition_id', client['partition_id'])
        self.__setitem__('client_wrench_id', client['wrench_id'])


class LakeTreeUserTypeFactory(LakeFactory):
    id_ = fake.pyint
    description = fake.color_name


class LakeTreeUserStatusFactory(LakeFactory):
    id_ = fake.pyint
    description = fake.color_name


class LakeTreeOrderStatusFactory(LakeFactory):
    id_ = fake.pyint
    description = fake.color_name


class LakeTreeOrderTypeFactory(LakeFactory):
    id_ = fake.pyint
    description = fake.color_name


class LakePyrRankDefinitionFactory(LakeFactory):
    id_ = fake.pyint
    name = fake.color_name
    level = fake.pyint
    client_level = fake.pyint


for c in FactoryRegistry.icentris_clients:
    FactoryRegistry.create_multiple(
        LakeTreeUserTypeFactory,
        len(c['tree_user_types']),
        [(lambda f: f.update({'icentris_client': c['icentris_client']}) or f)(f) for f in c['tree_user_types']])
    FactoryRegistry.create_multiple(
        LakeTreeUserStatusFactory,
        len(c['tree_user_statuses']),
        [(lambda f: f.update({'icentris_client': c['icentris_client']}) or f)(f) for f in c['tree_user_statuses']])
    FactoryRegistry.create_multiple(
        LakePyrRankDefinitionFactory,
        len(c['pyr_rank_definitions']),
        [(lambda f: f.update({'icentris_client': c['icentris_client']}) or f)(f) for f in c['pyr_rank_definitions']])
    FactoryRegistry.create_multiple(
        LakeTreeOrderTypeFactory,
        len(c['tree_order_types']),
        [(lambda f: f.update({'icentris_client': c['icentris_client']}) or f)(f) for f in c['tree_order_types']])
    FactoryRegistry.create_multiple(
        LakeTreeOrderStatusFactory,
        len(c['tree_order_statuses']),
        [(lambda f: f.update({'icentris_client': c['icentris_client']}) or f)(f) for f in c['tree_order_statuses']])


class LakeTreeUserFactory(LakeFactory):
    id_ = fake.pyint
    parent_id = fake.pyint
    sponsor_id = fake.pyint
    first_name = fake.first_name
    last_name = fake.last_name
    email = fake.company_email
    user_type_id = [fake.random_relation_id, LakeTreeUserTypeFactory, '__icentris_client__']
    user_status_id = [fake.random_relation_id, LakeTreeUserStatusFactory, '__icentris_client__']
    rank_id = [fake.random_relation_value, LakePyrRankDefinitionFactory, 'level', '__icentris_client__']
    paid_rank_id = [fake.random_relation_value, LakePyrRankDefinitionFactory, 'level', '__icentris_client__']
    created_date = fake.past_datetime_as_str
    zip_ = fake.postcode


class LakeUserFactory(LakeFactory):
    id_ = fake.pyint
    email = fake.company_email
    first_name = fake.first_name
    last_name = fake.last_name
    failed_attempts = fake.pyint
    encrypted_password = fake.password
    tree_user_id = [fake.random_relation_id, LakeTreeUserFactory, '__icentris_client__']


class StagingUserFactory(StagingFactory):
    tree_user_id = fake.pyint
    user_id = fake.pyint
    parent_id = fake.pyint
    sponsor_id = fake.pyint
    client_type = [fake.random_relation_value, LakeTreeUserTypeFactory, 'description', '__icentris_client__']
    type_ = [fake.random_element, FactoryRegistry.normalized_types]
    client_status = [fake.random_relation_value, LakeTreeUserStatusFactory, 'description', '__icentris_client__']
    status = [fake.random_element, FactoryRegistry.normalized_statuses]
    client_lifetime_rank = [fake.random_relation_value, LakePyrRankDefinitionFactory, 'name', '__icentris_client__']
    client_paid_rank = [fake.random_relation_value, LakePyrRankDefinitionFactory, 'name', '__icentris_client__']
    zip_ = fake.postcode


class LakeTreeOrderFactory(LakeFactory):
    tree_user_id = [fake.random_relation_id, LakeTreeUserFactory, '__icentris_client__']
    id_ = fake.pyint
    order_date = fake.past_datetime_as_str
    created_date = fake.past_datetime_as_str
    order_type_id = [fake.random_relation_value, LakeTreeOrderTypeFactory, 'id', '__icentris_client__']
    order_status_id = [fake.random_relation_value, LakeTreeOrderStatusFactory, 'id', '__icentris_client__']


class LakePyrContactsFactory(LakeFactory):
    id_ = fake.pyint
    tree_user_id = fake.pyint
    first_name = fake.first_name
    last_name = fake.last_name


class LakePyrContactCategoriesFactory(LakeFactory):
    id_ = fake.pyint
    category_name = fake.color_name


class LakePyrContactsContactCategoriesFactory(LakeFactory):
    id_ = fake.pyint


class LakePyrContactEmailsFactory(LakeFactory):
    id_ = fake.pyint
    email = fake.color_name


class LakePyrContactPhoneNumbersFactory(LakeFactory):
    id_ = fake.pyint
    phone_number = fake.color_name
