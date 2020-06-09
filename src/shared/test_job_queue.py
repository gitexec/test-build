from .job_queue import JobQueue


class _JobDict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__

    def result(self):
        return []

    def done(self):
        return self.__getattr__('state') == 'DONE'


class MockClient():

    def create_job(self):
        return _JobDict({'job_id': 'job1', 'location': 'local', 'state': 'RUNNING'})

    def get_job(self, job_id, location):
        assert job_id == 'job1'
        assert location == 'local'
        return _JobDict({'job_id': 'job1', 'location': 'local', 'state': 'DONE'})


def test_func_args_only():
    def _func(one, two, three, expected):
        assert one == expected[0]
        assert two == expected[1]
        assert three == expected[2]

        return MockClient().create_job()

    q = JobQueue(MockClient(), _func)

    q.append('one', 2, None, ('one', 2, None))
    q.append(1, 'two', 3, (1, 'two', 3))

    q.run()


def test_func_kwargs_only():
    def _func(key1, key2, key3, expected):
        assert key1 == expected[0]
        assert key2 == expected[1]
        assert key3 == expected[2]

        return MockClient().create_job()

    q = JobQueue(MockClient(), _func)

    q.append(key1='one', key2=2, key3=None, expected=('one', 2, None))
    q.append(key1=1, key2='two', key3=3, expected=(1, 'two', 3))

    q.run()


def test_func_args_and_kwargs():
    def _func(key1, key2, key3, expected):
        assert key1 == expected[0]
        assert key2 == expected[1]
        assert key3 == expected[2]

        return MockClient().create_job()

    q = JobQueue(MockClient(), _func)

    q.append('one', 2, key3=None, expected=('one', 2, None))
    q.append(1, 'two', key3=3, expected=(1, 'two', 3))

    q.run()
