class TorobaseEventReader:
    def __init__(self, filename):
        self.filename = filename
        self.records = []

    def read_events(self):
        with open(self.filename, 'r') as file:
            lines = file.readlines()

        for i in range(0, len(lines), 6):
            record = {
                "type": lines[i].strip(),
                "transfer": lines[i + 1].strip(),
                "trade": lines[i + 3].strip(),
                "timestamp": lines[i + 4].strip(),
            }

            # Parsing transfer details
            transfer_details = record["transfer"].split(";")
            transfer_dict = {}
            for detail in transfer_details:
                key, value = [item.strip() for item in detail.split(":")]
                transfer_dict[key] = value
            record["transfer"] = transfer_dict

            # Parsing timestamp details
            record["timestamp"] = {
                "timestamp": record["timestamp"],
                "id": lines[i + 5].strip()
            }

            # Only parsing trade details if it's not 'N/A'
            if record["trade"] != 'N/A':
                trade_details = record["trade"].split(";")
                trade_dict = {}
                for detail in trade_details:
                    key, value = [item.strip() for item in detail.split(":")]
                    trade_dict[key] = value
                record["trade"] = trade_dict

            self.records.append(record)

    def display_records(self):
        for record in self.records:
            print(record)