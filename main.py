import argparse
import logging
import m3u8
import queue
import requests
import threading
import time

from datetime import datetime

q = queue.Queue()
hmap = {}

class PlaylistFetcherThread(threading.Thread):
  def __init__(self, threadID, name, playlist_url, output_dir):
    super(PlaylistFetcherThread, self).__init__()
    self.threadID = threadID
    self.name = name
    self.playlist_url = playlist_url
    self.playlist_filename = 'chunklist.m3u8'
    self.output_dir = output_dir

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

      playlist.dump(self.output_dir + self.playlist_filename)
      time.sleep(playlist.target_duration)


class DownloaderThread(threading.Thread):
  def __init__(self, threadID, name, output_dir):
    super(DownloaderThread, self).__init__()
    self.threadID = threadID
    self.name = name
    self.ts_list_filename = 'list.txt'
    self.counter = 0
    self.write_to_list_batch_size = 1
    self.batch_list = []
    self.num_retries = 3
    self.output_dir = output_dir

  def run(self):
    while True:
      filename = self.dequeue_download()
      self.insert_to_batch(filename)

  def dequeue_download(self):
    base_uri, filename = q.get(block=True, timeout=30)

    success = False
    for i in range(self.num_retries):
      resp = requests.get(base_uri + filename)
      logging.info("Consumer - Request to %s (Retries: %d)" % (base_uri+filename, i))

      if resp.status_code == 200:
        success = True
        break

    if not success:
      raise "Error after attempting to retry"

    with open(self.output_dir + (filename.split('/')[-1]), 'wb') as fd:
      for chunk in resp.iter_content(chunk_size=128):
        fd.write(chunk)

    q.task_done()

    return filename

  def insert_to_batch(self, ts_filename):
      self.counter += 1
      self.batch_list.append(ts_filename)

      # flush into output list ts file for each batch
      if self.counter % self.write_to_list_batch_size == 0:
        self.dump_batch_to_file()
        self.batch_list.clear()

  def dump_batch_to_file(self):
      with open(self.output_dir + self.ts_list_filename, 'a+') as list_fd:
        for filename in self.batch_list:
          list_fd.write("file '%s'\n" % (filename))


def main():
  now = datetime.now()

  parser = argparse.ArgumentParser(description='Download a HLS stream video, given a m3u8 playlist path.')
  parser.add_argument('playlist_url', type=str,
                      help='The m3u8 playlist URL path.')
  parser.add_argument('--odir', type=str,
                      default="./%s/" % (now.strftime("%Y%m%d%H%M%S")),
                      help='The path where the output will be stored.')
  parser.add_argument('--start_at', type=int, default=0,
                      help='If set, the fetch will be delayed until a given time')

  args = parser.parse_args()

  t1 = DownloaderThread(1, "downloader", args.odir)
  t2 = PlaylistFetcherThread(2, "fetcher", args.playlist_url, args.odir)

  logging.basicConfig(level=logging.INFO)

  while args.start_at > datetime.now().timestamp():
    logging.info("Wait until %d (current time: %f)" % (args.start_at, datetime.now().timestamp()))
    time.sleep(1)

  t1.start()
  t2.start()


if __name__ == '__main__':
  main()
