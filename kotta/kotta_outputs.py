import urllib.request
import shutil
import re
import os
import urllib.error
import logging
from urllib.request import urlretrieve, urlopen
from urllib.parse   import urlparse

logger  = logging.getLogger(__name__)

class KOut(object):    
    @staticmethod
    def extract_s3_url(url):
        if "s3.amazonaws.com" in url:
            t_url = url[8:]            
            parsed = urlparse(url)
            s3_key = parsed.path
            s3_bucket = parsed.netloc.replace(".s3.amazonaws.com", "")        
            return "s3://{0}{1}".format(s3_bucket, s3_key)

        elif re.search("https://s3.*amazonaws.com/", url):
            s3_path = re.sub("https://s3.*amazonaws.com/", "", url)
            tmp     = s3_path.split('/', 1)
            s3_bucket = tmp[0]
            s3_key    = tmp[1]
            return "s3://{0}/{1}".format(s3_bucket, s3_key)

        else:
            logger.warn("Unknown URL type.")
            print("Type 3")
            return url
        
    def __init__ (self, filestring):

        logger.debug("Creating object for {0}".format(filestring))
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
        return self.__url
    
    @property
    def s3_url(self):
        return self.__s3_url

    def get_data(self):
        if self.url:
            return urlretrieve(self.url, self.file)
        else:
            logger.warn("[WARN] File {0} was not generated on Kotta. Nothing to fetch".format(self.file))
            return False

        return 
        
    @property
    def file(self):        
        return self.__file

    def read(self):
        if self.url:
            return urlopen(self.url).read().decode('utf-8')
        else:
            logger.warn("Url not available for read")
            return None        
    
    def fetch(self):
        if self.url:
            return urlretrieve(self.url, self.file)
        else:
            logger.warn("File {0} was not generated on Kotta. Nothing to fetch".format(self.file))
            return False
    

    def __str__ (self):
        return "{0}({1})".format(self.__class__, self.__file)

    def __repr__ (self):
        return "{0}({1})".format(self.__class__, self.__file)
