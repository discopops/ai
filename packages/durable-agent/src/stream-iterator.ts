export async function* streamToIterator<T>(
  stream: ReadableStream<T>,
): AsyncGenerator<T, void, unknown> {
  const reader = stream.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        return;
      }
      yield value;
    }
  } finally {
    reader.releaseLock();
  }
}

export function iteratorToStream<T>(
  iterator: AsyncIterable<T>,
  options?: { signal?: AbortSignal },
): ReadableStream<T> {
  const it = iterator[Symbol.asyncIterator]();

  return new ReadableStream<T>({
    async pull(controller) {
      if (options?.signal?.aborted) {
        controller.close();
        return;
      }

      const { done, value } = await it.next();
      if (done) {
        controller.close();
      } else {
        controller.enqueue(value);
      }
    },
    async cancel() {
      if (it.return) {
        await it.return();
      }
    },
  });
}
