from celery import Celery
from app import run_bot as run_bot_fn


app = Celery('tasks')


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(60.0, run_bot.s())


@app.task
def run_bot():
    run_bot_fn()
