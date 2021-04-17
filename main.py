import argparse
import hls_downloader
import logging
import time

from datetime import datetime

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

  q = hls_downloader.DownloaderQueue()

  downloader = hls_downloader.DownloaderThread(args.odir, q)
  fetcher = hls_downloader.PlaylistFetcherThread(args.playlist_url, args.odir, q)

  hd = hls_downloader.HLSDownloader(downloader=downloader, playlist_fetcher=fetcher)

  logging.basicConfig(level=logging.INFO)

  while args.start_at > datetime.now().timestamp():
    logging.info("Wait until %d (current time: %f)" % (args.start_at, datetime.now().timestamp()))
    time.sleep(1)

  hd.start()


if __name__ == '__main__':
  main()
