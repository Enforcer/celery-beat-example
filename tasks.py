import logging
import uuid
from contextlib import contextmanager
from random import randint
from time import sleep

from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded
from redis import StrictRedis


logger = logging.getLogger(__name__)
redis_connection = StrictRedis(host='redis')


app = Celery('tasks', broker='redis://redis:6379/0')
app.conf.task_routes = {
    'tasks.single_worker_example': {'queue': 'queue_for_single_worker'}
}


REMOVE_ONLY_IF_OWNER_SCRIPT = \
"""if redis.call("get",KEYS[1]) == ARGV[1] then
    return redis.call("del",KEYS[1])
else
    return 0
end
"""


@contextmanager
def redis_lock(lock_name, expires=60):
    random_value = str(uuid.uuid4())
    lock_acquired = bool(
        redis_connection.set(lock_name, random_value, ex=expires, nx=True)
    )
    logger.info(f'Lock acquired? {lock_name} for {expires} - {lock_acquired}')
    yield lock_acquired
    if lock_acquired:
        redis_connection.eval(REMOVE_ONLY_IF_OWNER_SCRIPT, 1, lock_name, random_value)
        logger.info(f'Lock {lock_name} released!')


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(4, simple_periodic_task.s(), name='scan for expired accounts every 4 hours')
    sender.add_periodic_task(6, single_worker_example.s(), name='execute task dedicated for a single worker')
    sender.add_periodic_task(10, time_limited_task.s(), name='execute time-limited task')
    sender.add_periodic_task(6, periodic_task_with_lock.s(), name='execute task with chance for lock acquire fail')


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


@app.task(soft_time_limit=5, time_limit=10)
def time_limited_task():
    """
    This task has time limits to ensure it will never work for
    longer than anticipated.

    soft_time_limit is a number of seconds when SoftTimeLimitExceeded
    is raised to give a moment to clean up.
    time_limit is a number of seconds after task is terminated unconditionally
    """
    will_sleep_for = randint(5, 15)
    logger.info(f'Work started (need {will_sleep_for}s to finish)')
    try:
        sleep(will_sleep_for)
    except SoftTimeLimitExceeded:
        logger.info('Oups, soft limit exceeded! Quickly, clean up!')
        sleep(will_sleep_for)


@app.task()
def periodic_task_with_lock():
    """
    This task uses redis_lock context manager to ensure it is executed
    only one at a time.
    """
    with redis_lock('periodic_task_with_lock_lock', 10) as acquired:
        if not acquired:
            logger.warning('Lock not acquired!')
            return

        will_sleep_for = randint(5, 8)
        logger.info(f'I will keep lock for {will_sleep_for}s')
        sleep(will_sleep_for)
