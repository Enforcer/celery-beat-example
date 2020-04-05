import logging
from time import sleep

from celery import Celery


logger = logging.getLogger(__name__)


app = Celery('tasks', broker='redis://redis:6379/0')
app.conf.task_routes = {
    'tasks.single_worker_example': {'queue': 'queue_for_single_worker'}
}


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(4, simple_periodic_task.s(), name='scan for expired accounts every 4 hours')
    sender.add_periodic_task(6, single_worker_example.s(), name='execute task dedicated for a single worker')


@app.task()
def simple_periodic_task():
    """
    This task is goes to default queue.
    It is executed by one of workers in the pool.
    """
    logger.info('Doing some work!')
    sleep(1)
    logger.info('Finished!')


@app.task()
def single_worker_example():
    """
    This task is executed in a dedicated worker with a single process.
    It goes to a separate queue to make sure no other worker can
    pick it up.
    """
    logger.info('Work started...')
    sleep(3)
    logger.info('Work finished!')
