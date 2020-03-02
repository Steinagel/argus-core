from urllib.parse import urlparse
from lxml import html
from validators import url as valid_url
import requests

REPLACES = [[' ', '%20']]

class Scrap:
    def __init__(self, url):
        self.__url          = url
        self.__links        = []
        self.__source_code  = None
        self.__html         = None
        self.__domain       = None
        self.__cont_type    = None
        self.__headers      = None

    def get_source_code(self):
        if self.__source_code is None:
            try:
                response = requests.get(self.__url)
            except:
                return None
            self.__source_code  = response.content
            self.__headers      = response.headers
            self.__cont_type    = self.__headers['Content-Type'] if 'Content-Type' in self.__headers.keys() else False
        return self.__source_code

    def get_links(self):
        source_code = self.__get_html()

        if not self.__links:
            a_elements = source_code.xpath("//a")

            for a_el in a_elements:
                a_child_url = self.__find_url__(a_el)
                if valid_url(a_child_url):
                    self.__links.append(a_child_url)

                a_child   = a_el.xpath("//*[@src]")

                for el_child in a_child:
                    el_child_url = self.__find_url__(el_child)
                    if valid_url(el_child_url):
                        self.__links.append(el_child_url)
        return self.__links

    def __get_html(self):
        if self.__html is None:
            if self.get_source_code() is None:
                return None
            self.__html = html.fromstring(self.get_source_code())
        return self.__html

    def __get_domain(self):
        if self.__domain is None:
            self.__domain = urlparse(self.__url).netloc
        return self.__domain

    def __transform_url__(self, url_):
        if url_.startswith('//'):
            url_ = "http:"+url_ 
        elif url_.startswith('/'):
            url_ = "http://"+self.__get_domain()+(url_)
        else:
            url_ = "http://"+self.__get_domain()+'/'+(url_)

        for rep in REPLACES:
            url_ = url_.replace(rep[0], rep[1])

        return url_

    def __find_url__(self, node):
        node_url = ''
        if node.xpath('@src'):
            node_url = self.__transform_url__(node.xpath('@src')[0])
        return node_url
