import asyncio

from rclpy.client import Client


class AsyncioClient(Client):
    def __init__(self):
        self._pending_requests = {}  # sequence_number -> Future

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._destroyed:
            raise RuntimeError("Unable to get the next response: client destroyed")
        if self._closing:
            raise StopAsyncIteration

        return await self._receive_response()  # returns (response, sequence_number)

    def close(self):
        """Signal the client to stop receiving responses gracefully."""
        self._closing = True

    def __enter__(self):
        if self._closing:
            raise RuntimeError("Unable to enter a closed service")
        
        return self

    def __exit__(self, *exc):
        self.handle.clear_on_new_response_callback()
        for future in self._pending_requests.values():
            future.cancel()
        self._pending_requests.clear()
        super().__exit__(*exc)

    async def call(self, request):
        """Send request and await response."""
        if self._destroyed:
            raise RuntimeError("Unable to call service: client destroyed")

        future = asyncio.get_running_loop().create_future()
        sequence_number = self._handler.send_request(request)
        self._pending_requests[sequence_number] = future
        try:
            return await future
        finally:
            self._pending_requests.pop(sequence_number, None)
