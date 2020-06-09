
def test_dag_does_something_cool(setup):
    dag_bag = setup('vibe_to_lake')
    dag = dag_bag.get_dag('vibe_to_lake-bluesun')
    assert len(dag.tasks) == 58
