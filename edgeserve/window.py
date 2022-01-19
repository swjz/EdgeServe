class Window:
    def __init__(self, size):
        self.window = []
        self.size = size

    def gate_out(self, data):
        self.window.append(data)
        output = ''
        if self.size <= len(self.window):
            output = str(self.window)
            self.window = []
        return output.encode('utf-8')
