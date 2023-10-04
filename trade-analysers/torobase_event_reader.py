import utils


class TorobaseEventReader:
    def __init__(self):
        self.filename = utils.PYSCALPIE_PATH / "torobase" / "events.txt"

    def read_events(self):
        result_dict = {}
        with open(self.filename, 'r') as file:
            lines = file.readlines()

        for i in range(0, len(lines), 6):
            event_type = lines[i].strip()
            if event_type != "Trade Opened":
                continue

            description = lines[i + 1].strip()
            uid = lines[i + 3].strip()
            timestamp = lines[i + 4].strip()

            # Parsing transfer details
            description_details = description.split(";")

            # Type: Buy ; Market: EURUSD ; Amount: 344873 ; Open: 1.04761 ; TP: 1.04849 ; SL: 1.04673
            description_dict = {}
            for detail in description_details:
                key, value = [item.strip() for item in detail.split(":")]
                description_dict[key] = value

            event = {
                "take_profit": float(description_dict.get("TP", 0)) if description_dict.get("TP") else None,
                "stop_loss": float(description_dict.get("SL", 0)) if description_dict.get("SL") else None,
                "timestamp": utils.get_datetime_iso(timestamp)
            }

            result_dict[uid] = event
        return result_dict


if __name__ == '__main__':
    event_reader = TorobaseEventReader()
    events = event_reader.read_events()
