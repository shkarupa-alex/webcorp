# -*- coding: utf-8 -*-
import bs4
from newspaper import fulltext

def cleanup(html):
    soup = bs4.BeautifulSoup(html, 'lxml')

    for comment in soup.findAll(text=lambda node: isinstance(node, bs4.Comment)):
        comment.extract()
    for node in soup(['kbd', 'code', 'pre', 'samp', 'var', 'svg', 'script', 'style']):
        node.decompose()

    return str(soup)


def extract(html, language='ru'):
    try:
        text = fulltext(html=html, language=language)
    except:
        text = ''

    return text
