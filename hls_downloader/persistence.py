import queue

class QueueEmptyError(Exception):
  pass

class DownloaderQueue(object):
  def __init__(self, size=50):
    self.queue = queue.Queue(maxsize=size)
    self.hmap = {}

  def dequeue(self):
    try:
      download_uri, filename = self.queue.get(block=True, timeout=30)
      return download_uri, filename
    except queue.Empty:
      raise QueueEmptyError

  def enqueue(self, download_uri, filename):
    if filename in self.hmap:
      return False

    tup = (download_uri, filename)
    self.queue.put(tup)
    self.hmap[filename] = True

    return True

  def mark_as_completed(self):
    self.queue.task_done()