import enum
import queue
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, Future

logging.basicConfig(
    filename="error_log.txt",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s %(message)s"
)

class ProcessState(enum.Enum):
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    DONE    = "DONE"
    ERROR   = "ERROR"

class WorkState:
    def __init__(self, future: Future):
        self._future = future

    @property
    def state(self) -> ProcessState:
        if self._future.running():
            return ProcessState.RUNNING
        if self._future.cancelled():
            return ProcessState.ERROR
        if self._future.done():
            return ProcessState.DONE if self._future.exception() is None else ProcessState.ERROR
        return ProcessState.WAITING

    def result(self, timeout=None):
        return self._future.result(timeout)

class AsyncProcessor:
    _instance = None

    def __new__(cls, max_workers: int = 4):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, max_workers: int = 4):
        if getattr(self, "_initialized", False):
            return
        self._initialized = True

        self._task_queue = queue.Queue()
        self._executor   = ThreadPoolExecutor(max_workers=max_workers)
        self._shutdown   = threading.Event()
        threading.Thread(target=self._dispatcher, daemon=True).start()

    def submit(self, func, *args, **kwargs) -> WorkState:
        """작업을 큐에 넣고 WorkState(Future 래퍼)를 반환합니다."""
        promise = threading.Event()
        future = Future()

        def _wrapper():
            future.set_running_or_notify_cancel()
            try:
                result = func(*args, **kwargs)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)
                logging.error(f"Work failed: {func.__name__} args={args} kwargs={kwargs}", exc_info=e)

        self._task_queue.put(_wrapper)
        return WorkState(future)

    def _dispatcher(self):
        """큐에서 꺼낸 작업을 ThreadPoolExecutor에 넘깁니다."""
        while not self._shutdown.is_set():
            try:
                task = self._task_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            # Executor에 넘기면 자동으로 스레드풀 내 빈 슬롯을 관리
            self._executor.submit(task)

    def shutdown(self, wait=True):
        """새 작업 수신 중지, 기존 작업 완료 후 종료."""
        self._shutdown.set()
        if wait:
            self._executor.shutdown(wait=True)