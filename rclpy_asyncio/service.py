import asyncio

from rclpy.service import Service


class AsyncioService(Service):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._read_event = asyncio.Event()

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            request_and_header = self.handle.service_take_request(self.srv_type.Request)
            if request_and_header != (None, None):
                return request_and_header

            try:
                self._read_event.clear()
                await self._read_event.wait()
            except asyncio.CancelledError:
                raise StopAsyncIteration

    def _handle_new_request(self, _num_waiting_requests: int) -> None:
        asyncio.get_running_loop().call_soon_threadsafe(self._read_event.set)

    def __enter__(self):
        self.handle.set_on_new_request_callback(self._handle_new_request)
        return self

    def __exit__(self, *_exc):
        self.handle.clear_on_new_request_callback()
        super().destroy()

    def destroy(self):
        raise NotImplementedError("Use node.close_service to destroy the service object")

    def send_response(self, response, header):
        self.handle.service_send_response(response, header)
