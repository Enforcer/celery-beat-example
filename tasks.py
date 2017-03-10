from celery import Celery, chain


app = Celery('tasks', broker='redis://redis:6379/0')


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(10, scan_for_expired_users.s(), name='scan for expired accounts every 4 hours')


@app.task
def scan_for_expired_users():
    for user in get_expired_users():
        deactivating_process = chain(deactivate_account.s(user), send_expiration_email.s())
        deactivating_process()


@app.task
def deactivate_account(user):
    # do some stuff
    print(f'deactivating account: {user}')
    return user + '_deactivated'


@app.task
def send_expiration_email(user):
    # do some other stuff
    print(f'sending expiration email to: {user}')
    return True


def get_expired_users():
    return (f'user_{i}' for i in range(5))
