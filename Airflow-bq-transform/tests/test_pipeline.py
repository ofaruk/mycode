from airflow.models import DagBag
import unittest


file = "../dags/pipeline.py"


class TestPipelineDag(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(dag_folder=file, include_examples=False)

    # Test will check that Dag is not throwing any error.
    def test_import_dags(self):
        self.assertFalse(
            len(self.dagbag.import_errors),
            'DAG import failures. Errors: {}'.format(
                self.dagbag.import_errors
            )
        )

    # test will check correct dag contains in file
    def test_dags(self):
        """Check task in Dag of dag"""
        self.assertEqual(len(self.dagbag.dag_ids),1)
        self.assertEqual(self.dagbag.dag_ids.__contains__('composer_transform'),True)

    # test will count number of tasks in Dag
    def test_task_count(self):
        """Check task count of dag"""
        dag_id = 'composer_transform'
        dag = self.dagbag.get_dag(dag_id)
        self.assertEqual(len(dag.tasks), 4)


suite = unittest.TestLoader().loadTestsFromTestCase(TestPipelineDag)
unittest.TextTestRunner(verbosity=2).run(suite)
