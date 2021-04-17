import logging
import m3u8
import requests
import threading
import time

from hls_downloader import persistence

class PlaylistFetcherThread(threading.Thread):
  def __init__(self, playlist_url, output_dir, downloader_queue):
    super(PlaylistFetcherThread, self).__init__()
    self.playlist_url = playlist_url
    self.playlist_filename = 'chunklist.m3u8'
    self.output_dir = output_dir
    self.downloader_queue = downloader_queue

  def run(self):
    while True:
      playlist = m3u8.load(self.playlist_url)
      logging.info("PlaylistFetcherThread - Request to %s" % (self.playlist_url))
      if playlist.is_variant:
        self.playlist_url = playlist.base_uri + playlist.playlists[0].uri
        continue

      self.enqueue_playlist_files(playlist)

      playlist.dump(self.output_dir + self.playlist_filename)

      time.sleep(playlist.target_duration)

  def enqueue_playlist_files(self, playlist):
    for segment in playlist.segments:
      filename = segment.uri.replace('/', '-')
      download_uri = segment.base_uri + segment.uri
      if not self.downloader_queue.enqueue(download_uri, filename):
        logging.info("%s is already on the list" % (filename))
        continue


class DownloaderThread(threading.Thread):
  def __init__(self, output_dir, downloader_queue):
    super(DownloaderThread, self).__init__()
    self.ts_list_filename = 'list.txt'
    self.counter = 0
    self.write_to_list_batch_size = 1
    self.batch_list = []
    self.num_retries = 3
    self.output_dir = output_dir
    self.downloader_queue = downloader_queue

  def run(self):
    while True:
      filename = self.dequeue_download()
      self.insert_to_batch(filename)

  def dequeue_download(self):
    download_uri, filename = self.downloader_queue.dequeue()

    success = False
    for i in range(self.num_retries):
      resp = requests.get(download_uri)
      logging.info("DownloaderThread - Request to %s (Retries: %d)" % (download_uri, i))

      if resp.status_code == 200:
        success = True
        break

    if not success:
      raise "Error after attempting to retry"

    with open(self.output_dir + filename, 'wb') as fd:
      for chunk in resp.iter_content(chunk_size=128):
        fd.write(chunk)

    self.downloader_queue.mark_as_completed()

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