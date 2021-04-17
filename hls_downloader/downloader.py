import logging

class HLSDownloader(object):
  def __init__(self, downloader, playlist_fetcher):
    self.downloader = downloader
    self.playlist_fetcher = playlist_fetcher

  def start(self):
    self.downloader.start()
    self.playlist_fetcher.start()

    self.downloader.join()
    self.playlist_fetcher.join()

    logging.info("Download completed.")


