
template = 'vibe_to_bq_initial_load'


def test_load_dag(setup):
    dag_bag = setup(template)
    dag = dag_bag.get_dag('{}-bluesun'.format(template))
    assert len(dag.tasks) == 69
