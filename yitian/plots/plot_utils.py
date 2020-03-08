import re

import IPython
from IPython.display import HTML
from bs4 import BeautifulSoup


def get_html(plot_file: str, width=1500, height=900) -> HTML:
    """
    Generate an iframe for a specific plots file and return HTML.

    :param plot_file:
    :param width:
    :param height:

    :return:
    """
    iframe = '<iframe src="{src}" width="{width}" height="{height}"/>'.format(src=re.sub(r'^/cdn', '', plot_file),
                                                                              width=width,
                                                                              height=height)
    return HTML(iframe)


def add_subtitle(title, sub_title):
    return "{}<br><i>{}<i>}".format(title, sub_title)


def get_plot_path_from_iframe(plot_iframe: HTML) -> str:
    """
    Get the plots's path from the oFrame string

    :param plot_iframe: iFrame HTML

    :return: plots path
    """
    return BeautifulSoup(plot_iframe.data).iframe.get('src')
