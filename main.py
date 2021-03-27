import logging
import m3u8
import queue
import requests
import threading
import time

q = queue.Queue()
hmap = {}

class PlaylistFetcherThread(threading.Thread):
  def __init__(self, threadID, name, playlist_url):
    super(PlaylistFetcherThread, self).__init__()
    self.threadID = threadID
    self.name = name
    self.playlist_url = playlist_url

  def run(self):
    while True:
      playlist = m3u8.load(self.playlist_url)
      logging.info("Producer - Request to %s" % (self.playlist_url))
      if playlist.is_variant:
        self.playlist_url = playlist.base_uri + playlist.playlists[0].uri
        continue

      for segment in playlist.segments:
        ts_props = (segment.base_uri, segment.uri)
        if segment.uri in hmap:
          logging.info("%s is already on the list" % (segment.uri))
          continue

        q.put(ts_props)
        hmap[segment.uri] = True

      time.sleep(4)


class DownloaderThread(threading.Thread):
  def __init__(self, threadID, name):
    super(DownloaderThread, self).__init__()
    self.threadID = threadID
    self.name = name

  def run(self):
    while True:
      base_uri, filename = q.get(block=True, timeout=30)
      hmap.pop(filename)

      resp = requests.get(base_uri + filename)
      logging.info("Consumer - Request to %s" % (base_uri+filename))

      if resp.status_code != 200:
        # retry
        pass

      with open(filename, 'wb') as fd:
        for chunk in resp.iter_content(chunk_size=128):
          fd.write(chunk)

      q.task_done()

example = "https://hls-origin218.showroom-cdn.com/liveedge/ada318fccc18501f08e9e4689aaf8b7e854ac3b56050f7923999033ba6494436_source/chunklist.m3u8"

def main():
  t1 = DownloaderThread(1, "downloader")
  t2 = PlaylistFetcherThread(2, "fetcher", example)

  logging.basicConfig(level=logging.INFO)

  t1.start()
  t2.start()


if __name__ == '__main__':
  main()