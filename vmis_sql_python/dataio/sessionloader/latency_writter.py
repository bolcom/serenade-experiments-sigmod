class LatencyWriter:
    def __init__(self, outputfilename):
        self.file_handler = open(outputfilename, 'w')
        self.file_handler.write("position,latency_in_micros\n")

    def append_line(self, position, latency_in_micros):
        self.file_handler.write("{position},{latency_in_micros}\n".format(position=position,
                                                                          latency_in_micros=latency_in_micros))

    def close(self):
        self.file_handler.close()