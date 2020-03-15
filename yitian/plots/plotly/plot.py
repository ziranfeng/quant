import math
import time
from typing import List, Tuple

from IPython import display
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

from yitian.datasource import DATE
from yitian.plots import plot_utils
from yitian.plots.plotly import plotly_utils


def time_series(ts_pd: pd.DataFrame, cols: List[str], left_y_title: str, title: str, name: str,
                connectgaps=True, x_title='Date Time', right_cols: List[str]=None, right_y_title=None,
                min_time=None, max_time=None, width=1500, height=700, append_timestamp=False,
                plot_file='/home/jupyter/plots/time_series_{name}.html') -> display.HTML:

    if right_cols is None:
        right_cols = []

    plot_pd = ts_pd

    if min_time is not None:
        plot_pd = plot_pd[plot_pd.index.get_level_values(DATE) >= min_time]

    if max_time is not None:
        plot_pd = plot_pd[plot_pd.index.get_level_values(DATE) <= max_time]

    plot_pd = plot_pd.sort_index(level=DATE)

    fig = go.Figure() if len(right_cols) == 0 else make_subplots(specs=[[{'secondary_y': True}]])

    for left_col in cols:
        fig.add_trace(go.Scatter(
            x=plot_pd.index.get_level_values(DATE),
            y=plot_pd[left_col],
            mode='lines',
            name=left_col,
            connectgaps = connectgaps
        ))

    for right_col in right_cols:
        fig.add_trace(go.Scatter(
            x=plot_pd.index.get_level_values(DATE),
            y=plot_pd[right_col],
            mode='lines',
            name=right_col,
            connectgaps=connectgaps
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

    return plotly_utils.show(fig, plot_file, name, width=width, height=height, append_timestamp=append_timestamp)


def pie_chart(data_pd: pd.DataFrame, label_col: str, value_col: str, title: str, name: str,
              group_col: str=None, color_map=None, width=1500, height=700, append_timestamp=False,
              plot_file='/home/jupyter/plots/pie_chart_{name}.html') -> display.HTML:

    groups = [None] if group_col is None else data_pd[group_col].unique().tolist()

    rows = int(round(math.sqrt(len(groups))))
    cols = int(round(math.ceil(math.sqrt(len(groups)))))
    specs = [[{'type': 'domain'} for _ in range(cols)] for _ in range(rows)]

    fig = make_subplots(rows=rows, cols=cols, subplot_titles=groups, specs=specs)

    for idx, group in enumerate(groups):
        data_to_plot_pd = data_pd if group_col is None else data_pd[data_pd[group_col] == group]

        color_palette = None
        if color_map is not None:
            color_palette = [color_map.get(label) for label in data_to_plot_pd[label_col].unique()]

        new_trace = go.Pie(
            labels=data_to_plot_pd[label_col],
            values=data_to_plot_pd[value_col],
            name=group,
            marker={'colors': color_palette}
        )

        fig.add_trace(new_trace, int(math.floor(idx / float(len(groups) / rows))) + 1, idx % cols +1)

    fig.update_traces(hoverinfo='label+percent+name', textinfo='label+percent')
    fig.update_layout(title=go.layout.Title(text=title), font={'size': 12})

    return plotly_utils.show(fig, plot_file, name, width=width, height=height, append_timestamp=append_timestamp)


def plot_heatmap(heatmap_pd: pd.DataFrame, title: str, name: str, x_col: str=None, y_col: str=None, z_col: str=None,
                 x_title: str=None, y_title: str=None, colorbar_title: str=None, width: int=1500, height: int=700,
                 append_timestamp=False, plot_file='/home/jupyter/plots/heatmap_{name}.html') -> display.HTML:

    x = heatmap_pd.columns if x_col is None else heatmap_pd[x_col]
    y = heatmap_pd.index if y_col is None else heatmap_pd[y_col]
    z = heatmap_pd.values if z_col is None else heatmap_pd[z_col]

    x_title = x_title if x_title is not None else x_col if x_col is not None else heatmap_pd.columns.name
    y_title = y_title if y_title is not None else y_col if y_col is not None else heatmap_pd.index.name
    colorbar_title = colorbar_title if colorbar_title is not None else z_col if z_col is not None else 'values'

    heatmap = go.Heatmap(z=z, x=x, y=y, colorbar=dict(title=colorbar_title))

    fig = go.Figure(data=[heatmap])

    fig.update_layout(
        title=title,
        font=dict(size=12),
        xaxis_title=x_title,
        yaxis_title=y_title
    )

    return plotly_utils.show(fig, plot_file, name, width=width, height=height, append_timestamp=append_timestamp)


def plot_histogram(data_pd: pd.DataFrame, column: str, color_group: str, title: str, name: str,
                   width: int=1500, height: int=700, append_timestamp=False,
                   plot_file='/home/jupyter/plots/histogram_{name}.html') -> display.HTML:

    fig = px.histogram(data_pd, x=column, color=color_group, barmode='overlay', marginal='box',
                       category_orders={color_group: sorted(data_pd[color_group].unique())} , title=title)

    return plotly_utils.show(fig, plot_file, name, width=width, height=height, append_timestamp=append_timestamp)
