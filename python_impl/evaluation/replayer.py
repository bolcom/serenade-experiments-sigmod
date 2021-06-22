import numpy as np


class Replayer:

    def __init__(self, test_df):
        self.data = test_df
        self.data.sort_values(['SessionId', 'Time'], inplace=True)
        self.data = self.data.reset_index(drop=True)

        self.QTY_UNIQUE_SESSIONS = self.data['SessionId'].nunique()
        self.offset_sessions = np.zeros(self.QTY_UNIQUE_SESSIONS + 1, dtype=np.int32)
        self.length_session = np.zeros(self.QTY_UNIQUE_SESSIONS, dtype=np.int32)
        self.offset_sessions[1:] = self.data.groupby('SessionId').size().cumsum()
        self.length_session[0:] = self.data.groupby('SessionId').size()

        self.current_session_idx = 0
        self.pos = self.offset_sessions[self.current_session_idx]
        self.position = 0

    def next_sequence(self):
        """

        :yield: current_session_id SessionId of the ongoing session
                current_item_id ItemId the user is currently looking at
                ts Timestamp in seconds
                rest ItemIds that the user will interact with during this session
        """
        while self.current_session_idx != self.QTY_UNIQUE_SESSIONS:
            current_item_id = self.data['ItemId'][self.pos]
            current_session_id = self.data['SessionId'][self.pos]
            ts = self.data['Time'][self.pos]
            rest = self.data['ItemId'][
                   self.pos + 1:self.offset_sessions[self.current_session_idx] + self.length_session[self.current_session_idx]].values

            self.pos += 1
            self.position += 1

            if self.pos + 1 == self.offset_sessions[self.current_session_idx] + self.length_session[
                self.current_session_idx]:
                self.current_session_idx += 1

                self.pos = self.offset_sessions[self.current_session_idx]
                self.position = 0

            yield current_session_id, current_item_id, ts, rest
