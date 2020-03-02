from urllib.parse import urlparse
from lxml import html
import requests
from validators import url as valid_url
from config.scrap_config import *

class Scrap:
    def __init__(self, url, ScrapCode=True, ScrapMedia=False, LimitSize=None, MediaTypes=DEFAULT_TYPES, TryDomainOnHttpOff=False, HrefInitString=NOHTTPINIT):
        if not self.__validate_media_schema(MediaTypes):
            raise 'Invalid MediaType structure!'
        self.__url          = url
        self.__scrap_code   = ScrapCode
        self.__scrap_media  = ScrapMedia
        self.__size_limit   = LimitSize
        self.__mediatypes   = MediaTypes
        self.__use_domain   = TryDomainOnHttpOff
        self.__href_init    = HrefInitString
        self.__source_code  = None
        self.__domain       = None
        self.__links        = None
        self.__media_links  = None
        self.__media_chunks = None
        self.__pages_links  = None

    def get_source_code(self):
        if not self.__source_code:
            self.__source_code = html.fromstring(requests.get(self.__url).content)
        return self.__source_code

    def get_links(self, source_code=self.get_source_code()):
        if not self.__links:
            sources     = source_code.xpath("//*/@href") + source_code.xpath("//*/@src")
            all_urls    = []
            media_urls  = []
            pages_urls  = []
            for src in sources:
                if self.get_domain():
                    src = src if src[0:len(self.__href_init)] == self.__href_init else src
                    src = src if src[0] != '/' else src[1:]
                    src = src[0:-1] if src.endswith('/') else src
                    src = src if src.startswith("http") \
                               else self.get_domain + (src if src.startswith('/') else '/'+src)
                for rep in REPLACES:
                    src = src.replace(rep[0], rep[1])
                if valid_url(src):
                    fil = src.rsplit("/")[-1]
                    fmt = fil.rsplit('.', 1)[-1]
                    typ = [typkey for typkey in self.__mediatypes.keys() \
                                    if fmt in self.__mediatypes[typkey]['get']]
                    if not typ:
                        pages_urls.append(src)
                    else:
                        media_urls.append({"filename":fil, "format": fmt.lower(), "type": typ, "url": src, "size": __get_media_size(src)})
                    all_urls.append(src)
            self.__pages_links = pages_urls
            self.__media_links = media_urls
            self.__links = all_urls
        return self.__links

    def get_domain(self):
        if not self.__domain:
            self.__domain = urlparse(self.__url)
        return self.__domain

    def get_media_links(self):
        if self.__media_links is None:
            self.get_links()
        return self.__media_links

    def get_pages_links(self):
        if self.__pages_links is None:
            self.get_links()
        return self.__pages_links

    def analytic_media(self):
        pass

    def analytic_source_code(self):
        pass

    def __get_media_size(self, media_url):
        raw_data = requests.get(media_url, stream=True)
        if raw_data.status_code == 200 and 'Content-Length' in raw_data.headers.keys():
            return int(raw_data.headers['Content-Length'])
        return None

    def get_media_chunk(self, chunk_size=1024):
        '''
        '''
        if not self.__media_links:
            self.get_links()
        if not self.__media_chunks:
            media_chunks = []
            for media in self.__media_links:
                if not media["format"] in self.__mediatypes[media["type"]]['ignore'] and \
                    media["format"] in self.__mediatypes[media["type"]]['get'] and \
                    media["size"] and  \
                    media["size"] <= self.__mediatypes[media["type"]]["size_limit"]["size"] or \
                    not self.__mediatypes[media["type"]]["size_limit"]["yes"]:
                    media_request = requests.get(media["url"], stream=True)
                    media_request.raw.decode_content = True
                    # Iterate over iter_content(chunk_size)
                    # to save the files or stream them:
                    # for chunk in media_request.iter_content(chunk_size):
                    #        fd.write(chunk)
                    media_chunks.append(media_request)
            self.__media_chunks = media_chunks
        return self.__media_chunks


    def __validate_media_schema(self, MediaData, schema=DEFAULT_SCHEMA):
        try:
            schema.validate(MediaData)
            return True
        except SchemaError:
            return False
