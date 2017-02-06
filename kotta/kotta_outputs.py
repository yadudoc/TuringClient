""" KOut class manages kotta output objects.

Provides convenience function to download and view outputs

"""

import re
import logging
from urllib.request import urlretrieve, urlopen
from urllib.parse   import urlparse

logger  = logging.getLogger(__name__)

class KOut(object):
    """ KOut class encapsulated a Kotta Output file.
    This later should be converted to a future object.

    """

    @staticmethod
    def extract_s3_url(url):
        """ Parse S3 url from a string url to a uniform s3://<bucket>/<key> format

        """
        if "s3.amazonaws.com" in url:
            parsed    = urlparse(url)
            s3_key    = parsed.path
            s3_bucket = parsed.netloc.replace(".s3.amazonaws.com", "")
            return "s3://{0}{1}".format(s3_bucket, s3_key)

        elif re.search("https://s3.*amazonaws.com/", url):
            s3_path   = re.sub("https://s3.*amazonaws.com/", "", url)
            tmp       = s3_path.split('/', 1)
            s3_bucket = tmp[0]
            s3_key    = tmp[1]
            return "s3://{0}/{1}".format(s3_bucket, s3_key)

        else:
            logger.error("Unknown URL type.")
            return url

    def __init__ (self, filestring):
        """ Create a KOut object from a filestring
        """

        logger.debug("Creating object for %s", filestring)
        if filestring.startswith('<a href="') and filestring.endswith('</a>') :
            stripped = filestring[9:][:-4]
            self.__url, self.__file = stripped.split('">')
            self.__s3_url = self.extract_s3_url(self.__url)

        elif filestring.startswith('<i>') and filestring.endswith('</i>'):
            self.__url    = None
            self.__file   = filestring[3:][:-3]
            self.__s3_url = None

        else:
            self.__url    = None
            self.__file   = filestring
            self.__s3_url = None

    @property
    def url(self):
        """ Returns the url string
        """
        return self.__url

    @property
    def s3_url(self):
        """ Returns the S3 url
        """
        return self.__s3_url

    def get_data(self):
        """ Retrieve the data from the URL to the FILE
        """
        if self.url:
            return urlretrieve(self.url, self.file)
        else:
            logger.warning("[WARN] File %s was not generated on Kotta. Nothing to fetch", self.file)
            return False

        return

    @property
    def file(self):
        """ Returns the target filename on the local filesystem
        """
        return self.__file

    def read(self):
        """ Opens the remote url and returns the utf-8 decoded string
        """
        if self.url:
            return urlopen(self.url).read().decode('utf-8')
        else:
            logger.warning("Url not available for read")
            return None

    def fetch(self):
        """ Fetch the remote url to the local file
        """
        if self.url:
            return urlretrieve(self.url, self.file)
        else:
            logger.warning("File %s was not generated on Kotta. Nothing to fetch", self.file)
            return False


    def __str__ (self):
        """ String repr of the KOut obj
        """
        return "{0}({1})".format(self.__class__, self.__file)

    def __repr__ (self):
        """ Same as above
        """
        return "{0}({1})".format(self.__class__, self.__file)
