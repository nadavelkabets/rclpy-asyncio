import asyncio

from rclpy.client import Client


class AsyncioClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._read_event = asyncio.Event()
        self._pending_requests: dict[int, asyncio.Future] = {}

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            header_and_response = self.handle.take_response(self.srv_type.Response)
            if header_and_response != (None, None):
                header, response = header_and_response
                return response, header.request_id.sequence_number

            try:
                self._read_event.clear()
                await self._read_event.wait()
            except asyncio.CancelledError:
                raise StopAsyncIteration

    def _handle_new_response(self, _num_waiting: int) -> None:
        asyncio.get_running_loop().call_soon_threadsafe(self._read_event.set)

    def __enter__(self):
        self.handle.set_on_new_response_callback(self._handle_new_response)
        return self

    def __exit__(self, *_exc):
        self.handle.clear_on_new_response_callback()
        for future in self._pending_requests.values():
            future.cancel()
        self._pending_requests.clear()
        super().destroy()

    def destroy(self):
        raise NotImplementedError("Use node.close_client to destroy the client object")

    async def call(self, request):
        future = asyncio.get_running_loop().create_future()
        sequence_number = self.handle.send_request(request)
        self._pending_requests[sequence_number] = future
        try:
            return await future
        finally:
            self._pending_requests.pop(sequence_number, None)

    def get_pending_request(self, sequence_number):
        return self._pending_requests[sequence_number]
