import csv
from pathlib import Path

class Vizualization(object):
    CSV_DATA_FILE_PATH = Path(__file__).parent / 'data' / 'player_data.csv'
    CSV_DATA_OUTPUT_FILE_PATH = Path(__file__).parent / 'data' / 'player_data_output.csv'
    def __init__(self):
        with self.CSV_DATA_FILE_PATH.open(mode='r') as f:
            csv_reader = csv.reader(
                f,
                delimiter=','
            )
            print("hey", csv_reader)
            self.row_data = []
            i = 0
            for row in csv_reader:
                self.row_data.append(row)
        
        self.field_names = self.row_data[0]
        self.data = self.row_data[1:]
        self.build_field_index()

        self.clean_data()
        self.convert_height_into_float_meter()
        self.output_data_file([self.field_names]+ self.data)
    
    def build_field_index(self):
        self.field_dict = {}
        for index, field_string in enumerate(self.field_names):
            self.field_dict.update({
                field_string: index
            })
    
    def clean_data(self):
        self.data = list(filter(lambda d: d[ self.field_dict['height'] ], self.data ))
    
    def convert_height_into_float_meter(self):
        for d in self.data:
            print(f"height is '{d[ self.field_dict['height'] ]}'")
            feet, inch = [ int(v) for v in d[ self.field_dict['height'] ].split('-')[0:2]]
            inch += feet * 12
            centimeter = inch * 2.54
            meter = round(centimeter / 100.0, 4)
            d[ 
                self.field_dict['height'] 
            ] = meter

    def output_data_file(self, data):
        with self.CSV_DATA_OUTPUT_FILE_PATH.open(mode="w") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows(data)
        

if __name__ == "__main__":
    viz = Vizualization()