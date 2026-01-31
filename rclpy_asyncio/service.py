from rclpy.service import Service


class AsyncioService(Service):
    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._destroyed:
            raise RuntimeError("Unable to get the next request: service destroyed")
        if self._closing:
            raise StopAsyncIteration

        return await self._receive_request()  # returns (request, header)

    def close(self):
        """Signal the service to stop accepting new requests gracefully."""
        self._closing = True

    def __enter__(self):
        self._handler.set_on_new_request_callback()
        return self

    def __exit__(self, *_exc):
        self._handler.clear_on_new_request_callback()

    def send_response(self, response, header):
        self._handler.service_send_response(response, header)