from __future__ import annotations

from celery import Celery, Task
from celery.worker.request import Request

from utils.load_config import load_config
from utils.logger import get_logger

logger = get_logger("celery")


class AppRequest(Request):  # type: ignore
    "A minimal custom request to log failures and hard time limits."

    def on_timeout(self, soft: bool, timeout: float) -> None:
        super().on_timeout(soft, timeout)
        if not soft:
            logger.warning("A hard timeout was enforced for task %s", self.task.name)

    def on_failure(
        self,
        exc_info: tuple[type, ...],
        send_failed_event: bool = True,
        return_ok: bool = False,
    ) -> None:
        super().on_failure(exc_info, send_failed_event=send_failed_event, return_ok=return_ok)
        logger.warning("Failure detected for task %s", self.task.name)


class BaseTask(Task):  # type: ignore
    Request = AppRequest


configParser = load_config()
app = Celery(
    "app",
    broker=configParser.get("celery", "BrokerUrl", fallback="redis://localhost:6379/0"),
    backend=configParser.get("celery", "BackendUrl", fallback="redis://localhost:6379/0"),
    task_cls=BaseTask,
)

app.autodiscover_tasks(["app.background.tasks"])

# Configurable default queue so dev stacks sharing a broker stay isolated.
app.conf.task_default_queue = configParser.get("celery", "TaskQueue", fallback="celery")
