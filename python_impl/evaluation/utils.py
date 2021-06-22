from _datetime import timezone, datetime
import time
import numpy as np

class DataframeStatistics:

    @staticmethod
    def print_df_statistics(descriptive_name, input_df):
        data_start = datetime.fromtimestamp( input_df.Time.min(), timezone.utc )
        data_end = datetime.fromtimestamp( input_df.Time.max(), timezone.utc )
        print('Loaded {} set\n\tEvents: {}\n\tSessions: {}\n\tItems: {}\n\tSpan: {} / {}\n'.
          format(descriptive_name, len(input_df), input_df.SessionId.nunique(), input_df.ItemId.nunique(), data_start.date().isoformat(),
                 data_end.date().isoformat()))


class SimpleStopwatch:
    def __init__(self):
        self.prediction_durations_in_micros = []
        self.start_secs = 0

    def start(self):
        self.start_secs = time.time()

    def stop(self, position):
        duration = time.time() - self.start_secs
        self.prediction_durations_in_micros.append((position, int(round(duration * 1_000_000))))

    def get_percentile_in_micros(self, p):
        return np.percentile(self.prediction_durations_in_micros, p)

    def get_n(self):
        return len(self.prediction_durations_in_micros)

    def get_prediction_latencies_in_micros(self):
        return self.prediction_durations_in_micros
