from __future__ import absolute_import

import os
import time

from IPython.display import HTML
import plotly.io as pio

from yitian.plots import plot_utils

pio.templates.default = 'none'


def save_plot(fig, plot_file, **kwargs):
    """
    Create plot file directory if it doesn't exist before calling plotly `write_html`. The default theme is none

    :param fig:
    :param plot_file:
    :param kwargs:
    :return:
    """
    plot_dir = os.path.dirname(plot_file)
    if not os.path.exists(plot_dir):
        os.makedirs(plot_dir)

    pio.write_html(fig, plot_file, **kwargs)


def show(fig, plot_file='/home/jupyter/plots/{name}.html', name=None, append_timestamp=True, width=1500, height=900) -> HTML:
    """
    Display Plotly figure in the current workbench session

    :param fig:
    :param plot_file:
    :param name:
    :param append_timestamp:
    :param width:
    :param height:

    :return:
    """
    if append_timestamp:
        curren_time = "{time}".format(time=time.time()).replace('.', '')
        name = curren_time if name is None else '{name}_{time}'.format(name=name, time=curren_time)

    plot_file_path = plot_file.format(name=name)

    save_plot(fig, plot_file_path)

    return plot_utils.get_html(plot_file_path, width=width, height=height)
