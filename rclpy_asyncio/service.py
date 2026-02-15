import asyncio
from urllib import request

from rclpy.service import Service
from rclpy.executors import SingleThreadedExecutor

class AsyncioService(Service):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._read_event = asyncio.Event()
        self._closing = False
        self.handle.set_on_new_request_callback(self._handle_new_request)

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            if self._closing:
                raise StopAsyncIteration()
            request_and_header = self.handle.service_take_request(self.srv_type.Request)
            if request_and_header is not (None, None):
                break

            await self._read_event.wait()

        return request_and_header

    def close(self):
        """Signal the service to stop accepting new requests gracefully."""
        self._closing = True
        self._read_event.set()

    def __enter__(self):
        if self._closing:
            raise RuntimeError("Unable to enter a closed service")
        
        return self

    def _handle_new_request(self, _num_waiting_requests: int) -> None:
        asyncio.get_running_loop().call_soon_threadsafe(self._read_event.set)

    def __exit__(self, *exc):
        self._closing = True
        self.handle.clear_on_new_request_callback()
        super().__exit__(*exc)

    def send_response(self, response, header):
        self.handle.service_send_response(response, header)

    def destroy(self):
        raise NotImplementedError("Use node.close_service to destroy the service object")
