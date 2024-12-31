import numpy as np
import pandas as pd
import random


def generate_customer_profiles_table(n_customers, random_state=0):
    """
    It takes as input the number of customers for which to generate a profile and a random state for
    reproducibility. It returns a DataFrame containing the properties for each customer.
    """

    np.random.seed(random_state)

    customer_id_properties = []
    columns = [
        'CUSTOMER_ID',
        'x_customer_id',
        'y_customer_id',
        'mean_amount',
        'std_amount',
        'mean_nb_tx_per_day'
    ]

    # Generate customer properties from random distributions
    for customer_id in range(n_customers):
        x_customer_id = np.random.uniform(0, 100)
        y_customer_id = np.random.uniform(0, 100)

        mean_amount = np.random.uniform(5, 100)
        std_amount = mean_amount / 2

        mean_nb_tx_per_day = np.random.uniform(0, 4)

        customer_id_properties.append([
            customer_id,
            x_customer_id,
            y_customer_id,
            mean_amount,
            std_amount,
            mean_nb_tx_per_day
        ])

    customer_profiles_table = pd.DataFrame(customer_id_properties, columns=columns)

    return customer_profiles_table


def generate_terminal_profiles_table(n_terminals, random_state=0):
    """
    It takes as input the number of terminals for which to generate a profile and a random state
    for reproducibility. It returns a DataFrame containing the properties for each terminal.
    """

    np.random.seed(random_state)

    terminal_id_properties = []
    columns = ['TERMINAL_ID', 'x_terminal_id', 'y_terminal_id']

    # Generate terminal properties from random distributions
    for terminal_id in range(n_terminals):
        x_terminal_id = np.random.uniform(0, 100)
        y_terminal_id = np.random.uniform(0, 100)

        terminal_id_properties.append([terminal_id, x_terminal_id, y_terminal_id])

    terminal_profiles_table = pd.DataFrame(terminal_id_properties, columns=columns)

    return terminal_profiles_table


def get_list_terminals_within_radius(customer_profile, x_y_terminals, r):
    """
    The function will take as input a customer profile (any row in the customer profiles table), an array
    that contains the geographical location of  all terminals, and the radius r. It will return the list of
    terminals within a radius of r for that customer.
    """

    # Use numpy arrays in the following to speed up computations

    # Location (x,y) of customer as numpy array
    x_y_customer = customer_profile[['x_customer_id', 'y_customer_id']].values.astype(float)

    # Squared difference in coordinates between customer and terminal locations
    squared_diff_x_y = np.square(x_y_customer - x_y_terminals)

    # Sum along rows and compute squared root to get distance
    dist_x_y = np.sqrt(np.sum(squared_diff_x_y, axis=1))

    # Get the indices of terminals which are at a distance less than r
    available_terminals = list(np.where(dist_x_y < r)[0])

    # Convert all terminal indices to normal Python integers (int)
    available_terminals = [int(terminal) for terminal in available_terminals]

    # Return the list of terminal IDs
    return available_terminals


def generate_transactions_table(customer_profile, start_date, nb_days):
    """
    Takes as input a customer profile, a starting date, and a number of days for which to generate transactions.
    It will return a table of transactions, which follows the format presented above (without the transaction
    label, which will be added in fraud scenarios generation).
    """

    customer_transactions = []
    columns = ['TX_TIME_SECONDS', 'TX_TIME_DAYS', 'CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT']

    random.seed(int(customer_profile.CUSTOMER_ID))
    np.random.seed(int(customer_profile.CUSTOMER_ID))

    # For all days
    for day in range(nb_days):

        # Random number of transactions for that day
        nb_tx = np.random.poisson(customer_profile.mean_nb_tx_per_day)

        # If nb_tx positive, let us generate transactions
        if nb_tx > 0:

            for tx in range(nb_tx):

                # Time of transaction: Around noon, std 20000 seconds. This choice aims at simulating the fact that
                # most transactions occur during the day.
                time_tx = int(np.random.normal(86400 / 2, 20000))

                # If transaction time between 0 and 86400, let us keep it, otherwise, let us discard it
                if (time_tx > 0) and (time_tx < 86400):

                    # Amount is drawn from a normal distribution
                    amount = np.random.normal(customer_profile.mean_amount, customer_profile.std_amount)

                    # If amount negative, draw from a uniform distribution
                    if amount < 0:
                        amount = np.random.uniform(0, customer_profile.mean_amount * 2)

                    amount = np.round(amount, decimals=2)

                    if len(customer_profile.available_terminals) > 0:
                        terminal_id = random.choice(customer_profile.available_terminals)

                        customer_transactions.append([
                            time_tx + day * 86400, day,
                            customer_profile.CUSTOMER_ID,
                            terminal_id,
                            amount
                        ])

    customer_transactions = pd.DataFrame(customer_transactions, columns=columns)

    if len(customer_transactions) > 0:
        customer_transactions['TX_DATETIME'] = (
            pd.to_datetime(customer_transactions["TX_TIME_SECONDS"], unit='s', origin=start_date))
        customer_transactions = customer_transactions[
            ['TX_DATETIME', 'CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT', 'TX_TIME_SECONDS', 'TX_TIME_DAYS']]

    return customer_transactions


def add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df):
    """
    Scenario 1: Any transaction whose amount is more than 220 is a fraud. This scenario is not inspired by a
    real-world scenario. Rather, it will provide an obvious fraud pattern that should be detected by any baseline
    fraud detector. This will be useful to validate the implementation of a fraud detection technique.

    Scenario 2: Every day, a list of two terminals is drawn at random. All transactions on these terminals in the
    next 28 days will be marked as fraudulent. This scenario simulates a criminal use of a terminal, through phishing
    for example. Detecting this scenario will be possible by adding features that keep track of the number of
    fraudulent transactions on the terminal. Since the terminal is only compromised for 28 days, additional strategies
    that involve concept drift will need to be designed to efficiently deal with this scenario.

    Scenario 3: Every day, a list of 3 customers is drawn at random. In the next 14 days, 1/3 of their transactions
    have their amounts multiplied by 5 and marked as fraudulent. This scenario simulates a card-not-present fraud
    where the credentials of a customer have been leaked. The customer continues to make transactions, and
    transactions of higher values are made by the fraudster who tries to maximize their gains. Detecting this
    scenario will require adding features that keep track of the spending habits of the customer. As for scenario 2,
    since the card is only temporarily compromised, additional strategies that involve concept drift should also
    be designed.
    """

    # By default, all transactions are genuine
    transactions_df['TX_FRAUD'] = 0
    transactions_df['TX_FRAUD_SCENARIO'] = 0

    # Scenario 1
    transactions_df.loc[transactions_df.TX_AMOUNT > 220, 'TX_FRAUD'] = 1
    transactions_df.loc[transactions_df.TX_AMOUNT > 220, 'TX_FRAUD_SCENARIO'] = 1

    # Scenario 2
    for day in range(transactions_df.TX_TIME_DAYS.max()):
        compromised_terminals = terminal_profiles_table.TERMINAL_ID.sample(n=2, random_state=day)

        compromised_transactions = transactions_df[(transactions_df.TX_TIME_DAYS >= day) &
                                                   (transactions_df.TX_TIME_DAYS < day + 28) &
                                                   (transactions_df.TERMINAL_ID.isin(compromised_terminals))]

        transactions_df.loc[compromised_transactions.index, 'TX_FRAUD'] = 1
        transactions_df.loc[compromised_transactions.index, 'TX_FRAUD_SCENARIO'] = 2

    # Scenario 3
    for day in range(transactions_df.TX_TIME_DAYS.max()):
        compromised_customers = customer_profiles_table.CUSTOMER_ID.sample(n=3, random_state=day).values

        compromised_transactions = transactions_df[(transactions_df.TX_TIME_DAYS >= day) &
                                                   (transactions_df.TX_TIME_DAYS < day + 14) &
                                                   (transactions_df.CUSTOMER_ID.isin(compromised_customers))]

        nb_compromised_transactions = len(compromised_transactions)

        random.seed(day)
        index_frauds = random.sample(list(compromised_transactions.index.values),
                                     k=int(nb_compromised_transactions / 3))

        transactions_df.loc[index_frauds, 'TX_AMOUNT'] = transactions_df.loc[index_frauds, 'TX_AMOUNT'] * 5
        transactions_df.loc[index_frauds, 'TX_FRAUD'] = 1
        transactions_df.loc[index_frauds, 'TX_FRAUD_SCENARIO'] = 3

    return transactions_df
