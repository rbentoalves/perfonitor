import matplotlib.pyplot as plt


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
