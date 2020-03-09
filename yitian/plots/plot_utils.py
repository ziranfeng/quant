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
    # TODO: On GCP Jupyter NoteBook the iframe cannot be rendered if it stored in porject file system
    #       And the Jupyter doesn't have the permission to access the root system
    #       Therefore, the work around is to load the local HTML directly from Jupyter local system
    iframe = '<iframe src="{src}" width="{width}" height="{height}"/>'.format(src=plot_file,
                                                                              width=width,
                                                                              height=height)
    return HTML(plot_file)


def add_subtitle(title, sub_title):
    return "{}<br><i>{}<i>}".format(title, sub_title)


def get_plot_path_from_iframe(plot_iframe: HTML) -> str:
    """
    Get the plots's path from the oFrame string

    :param plot_iframe: iFrame HTML

    :return: plots path
    """
    return BeautifulSoup(plot_iframe.data).iframe.get('src')
