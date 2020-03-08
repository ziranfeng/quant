import math
import time
from typing import List, Tuple

from IPython import display
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from yitian.datasource.quandl import DATE
from yitian.plots import plot_utils
from yitian.plots.plotly import plotly_utils


def time_series(data_pd, cols: List[str], left_y_title: str, title: str, x_title='Date Time', target_id = None,
                right_cols: List[str] = None, right_y_title=None, min_time=None, max_time=None, name: str = None,
                width=1500, height=700, plot_file='/cdn/plots/plotly/time_{name}.html') -> display.HTML:

    if right_cols is None:
        right_cols = []

    plot_pd = data_pd.loc[target_id] if target_id is not None else data_pd

    if min_time is not None:
        plot_pd = plot_pd[plot_pd.index.get_level_values(DATE) >= min_time]

    if max_time is not None:
        plot_pd = plot_pd[plot_pd.index.get_level_values(DATE) <= max_time]

    plot_pd = plot_pd.sort_index(level=DATE)

    fig = go.Figure() if len(right_cols) == 0 else make_subplots(specs=[{'secondary_y': True}])

    for left_col in cols:
        fig.add_trace(go.Scatter(
            x=plot_pd.index.get_level_values(DATE),
            y=plot_pd[left_col],
            mode='lines',
            name=left_col
        ))

    for right_col in right_cols:
        fig.add_trace(go.Scatter(
            x=plot_pd.index.get_level_values(DATE),
            y=plot_pd[right_col],
            mode='lines',
            name=left_col
        ), secondary_y=True)

    fig.update_layout(
        title=title,
        xaxis_title=x_title,
    )

    if (len(right_cols) > 0) & (right_y_title is not None):
        fig.update_yaxes(title_text=left_y_title, secondary_y=False, rangemode='tozero')
        fig.update_yaxes(title_text=right_y_title, secondary_y=True, rangemode='tozero')
    else:
        fig.update_yaxes(title_text=left_y_title)

    return plotly_utils.show(fig, plot_file, name, width=width, height=height)
