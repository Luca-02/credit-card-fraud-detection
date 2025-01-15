import os
import shutil

import matplotlib.pyplot as plt

from common.logger import logger


def clear_dir_path(path: str):
    """
    Delete all contents in the specified path.

    :param path: Path to delete all content.
    """

    if os.path.exists(path):
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                logger.error('Failed to delete %s. Reason: %s' % (file_path, e))


def create_plot(analysis_path: str, time_results: dict):
    """
    Create a plot for the execution times passed.

    :param analysis_path: Output path where the plot image will be stored.
    :param time_results: Executions time dictionary.
    """

    datasets = list(time_results.keys())
    load_times = [times["loading_time"] for times in time_results.values()]

    # Operation times for each operation
    operations = list(time_results[datasets[0]]["operations"].keys())
    operation_times = {
        operation: [times["operations"][operation] for times in time_results.values()] for operation in operations
    }

    # Plot loading times
    plt.figure(figsize=(10, 6))
    plt.bar(datasets, load_times, label="Loading Time")

    # Plot operations time
    bottom_values = load_times[:]
    for i, (op, times) in enumerate(operation_times.items()):
        plt.bar(datasets, times, bottom=bottom_values, label=f"Operation {op} Time")
        bottom_values = [b + t for b, t in zip(bottom_values, times)]

    plt.xlabel("Datasets")
    plt.ylabel("Time (seconds)")
    plt.title("Execution Times: Loading and Operations")
    plt.legend()
    plt.tight_layout()

    # Save plot as an image
    if not os.path.exists(analysis_path):
        os.makedirs(analysis_path)

    plot_path = os.path.join(analysis_path, "execution_times_plot.png")
    plt.savefig(plot_path)
    logger.info(f"Execution times plot saved at: {plot_path}")
