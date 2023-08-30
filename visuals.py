import matplotlib.pyplot as plt
import math


def availability_visuals(availability_fleet_per_period, period, folder_img):
    df = availability_fleet_per_period[period]
    df.index = df.index.astype('str')
    df_to_plot = df[df.index.str.contains('LSBP')].sort_index()
    df_to_plot_line = df[~df.index.str.contains('LSBP')]  # variable is a Series not a df

    if period == 'choose':
        title = df_to_plot.columns.to_list()[0]
    else:
        title = period

    # month = 9

    x_labels = df_to_plot.columns.to_list()
    y_values_labels = df_to_plot.index.to_list()
    # y_values_labels = [name.replace('Corrected (w/clipping) Monthly', '').replace(' PR %','') for name in y_values_labels]
    colors = ['#FE5000' if "LSBP" in name else '#FF5353' for name in y_values_labels]
    y_values = df_to_plot[x_labels[0]] * 100
    y_values_lines = df_to_plot_line[x_labels[0]] * 100

    plt.figure(figsize=(27, 9))
    plt.style.use('ggplot')
    plt.suptitle(str(title.upper()) + ' Availability %', fontsize='xx-large')
    plt.ylabel('Availability %', fontsize='xx-large')
    plt.bar(y_values_labels, y_values, width=0.6, color=colors)
    plt.xticks(rotation=45, ha='right', fontsize='xx-large')
    plt.yticks(fontsize='xx-large')
    plt.ylim([0, 100])
    for index, data in enumerate(y_values):
        label = str(data)[:5] + "%"
        plt.text(x=index - 0.25, y=data + 1, s=label, fontdict=dict(fontsize=18))

    # Fleet line
    plt.axhline(y=y_values_lines['Fleet'], linewidth=2, color='black', linestyle='-.', label='Fleet')
    plt.text(-1.4, y_values_lines['Fleet'], s="{:.2%}".format(y_values_lines['Fleet'] / 100),
             fontdict=dict(fontsize=15))

    # Company goal line
    plt.axhline(y=y_values_lines['Company goal'], linewidth=2, color='red', linestyle='-.', label='Company goal')
    plt.text(-1.4, y_values_lines['Company goal'], s="{:.2%}".format(y_values_lines['Company goal'] / 100),
             fontdict=dict(fontsize=15))

    # Company max goal line
    plt.axhline(y=y_values_lines['Company max goal'], linewidth=2, color='green', linestyle='-.',
                label='Company max goal')
    plt.text(-1.4, y_values_lines['Company max goal'], s="{:.2%}".format(y_values_lines['Company max goal'] / 100),
             fontdict=dict(fontsize=15))

    plt.legend()

    period_graph = (folder_img + '/' + str(period.upper()) + '_availability.png')
    plt.savefig(period_graph, bbox_inches='tight')

    return period_graph


def clipping_visuals(summaries, folder_img, site):
    # Energy Clipped
    graphs = {}
    graphs_by_type = {}
    graphs_by_site = {}

    for key in summaries.keys():

        df = summaries[key]

        to_plot = ['Power Clipped', "Corrected Power Clipped"]

        x_label = df.index.name
        x_data = df.index
        y_label = "Energy Clipped kWh"
        """y_data = daily_summary[['Power Clipped', "Corrected Power Clipped"]]"""

        # x_label = df_name.replace(" over time","")

        plt.figure(figsize=(25, 10))
        plt.style.use('ggplot')

        plt.suptitle("Energy Clipped", fontsize='xx-large')

        plt.xticks(rotation=45, ha='right', fontsize='xx-large')
        plt.yticks(ha='right', fontsize='xx-large')
        plt.ylabel(y_label, fontsize='xx-large')

        for graph in to_plot:
            y_data = df[graph].values
            plt.plot(x_data, y_data, label=graph.replace('Power', "Energy"))

        period_graph = (folder_img + '/' + str(site.upper()) + '_energy_loss' + x_label + '.png')
        plt.savefig(period_graph, bbox_inches='tight')
        graphs[key] = period_graph

        plt.legend(fontsize='xx-large')
        # plt.show
        plt.close()

    graphs_by_type["Energy"] = graphs

    # % of loss
    graphs = {}
    for key in summaries.keys():

        df = summaries[key]

        to_plot = ['% of loss', "% of loss corrected"]

        x_label = df.index.name
        x_data = df.index
        y_label = "% Energy Clipped"
        """y_data = daily_summary[['Power Clipped', "Corrected Power Clipped"]]"""

        # x_label = df_name.replace(" over time","")

        plt.figure(figsize=(25, 10))
        plt.style.use('ggplot')

        plt.suptitle("Energy Clipped", fontsize='xx-large')

        plt.xticks(rotation=45, ha='right', fontsize='xx-large')
        plt.yticks(ha='right', fontsize='xx-large')
        plt.ylabel(y_label, fontsize='xx-large')

        for graph in to_plot:
            y_data = df[graph].values
            y_ticks = list(range(0, math.ceil(float(df[graph].max())) + 1))
            plt.yticks(y_ticks, y_ticks, ha='right', fontsize='xx-large')

            plt.plot(x_data, y_data, label=graph.replace('Power', "Energy"))

        plt.legend(fontsize='xx-large')

        period_graph = (folder_img + '/' + str(site.upper()) + '_%_of_loss' + x_label + '.png')
        plt.savefig(period_graph, bbox_inches='tight')
        graphs[key] = period_graph

        # plt.show
        plt.close()

    graphs_by_type["% of loss"] = graphs

    graphs_by_type


    return graphs_by_type