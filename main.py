import csv
from pathlib import Path

class Vizualization(object):
    CSV_DATA_DIRECTORY = Path(__file__).parent / 'data'
    CSV_DATA_FILE_PATH = CSV_DATA_DIRECTORY / 'player_data.csv'
    
    CSV_DATA_OUTPUT_CLEANED_FILE_PATH = CSV_DATA_DIRECTORY / 'cleaned' / 'cleaned_player_data_output.csv'
    CSV_DATA_OUTPUT_INVALID_FILE_PATH = CSV_DATA_DIRECTORY / 'cleaned' / 'invalid_player_data_output.csv'
    CSV_DATA_OUTPUT_PROCESSED_FILE_PATH = CSV_DATA_DIRECTORY / 'processed' / 'processed_player_data.csv'

    FIELDS_REQUIRED = ['weight', 'height', 'position', 'year_start', 'year_end']

    def __init__(self):
        with self.CSV_DATA_FILE_PATH.open(mode='r') as f:
            csv_reader = csv.reader(
                f,
                delimiter=','
            )
            self.row_data = []
            i = 0
            for row in csv_reader:
                self.row_data.append(row)
        
        self.field_names = self.row_data[0]
        self.build_field_index()

        self.raw_data = self.row_data[1:]
        self.clean_data()

        self.convert_height_into_float_meter()
        self.output_data_file(self.processed_data, path=self.CSV_DATA_OUTPUT_PROCESSED_FILE_PATH)
    

    
    def build_field_index(self):
        self.field_dict = {}
        for index, field_string in enumerate(self.field_names):
            self.field_dict.update({
                field_string: index
            })
    
    def get_field_value(self, d, field_name):
        return d[self.field_dict.get(field_name, None)]
    
    def clean_data(self):
        # Policy one - all requred fields need to be valid
        self.cleaned_data = []
        self.invalid_data = []
        for d in self.raw_data:
            valid = True
            for required_field_name in self.FIELDS_REQUIRED:
                if not self.get_field_value(d, required_field_name):
                    valid = False
                    break
            if valid:
                self.cleaned_data.append(d)
            else:
                self.invalid_data.append(d)
        
        self.output_data_file(self.cleaned_data, path=self.CSV_DATA_OUTPUT_CLEANED_FILE_PATH)
        self.output_data_file(self.invalid_data, path=self.CSV_DATA_OUTPUT_INVALID_FILE_PATH)
    
    def convert_height_into_float_meter(self):
        self.processed_data = []
        for d in self.cleaned_data:
            processed_d = d.copy()

            feet, inch = [ int(v) for v in self.get_field_value(d, 'height').split('-')[0:2]]
            inch += feet * 12
            centimeter = inch * 2.54
            meter = round(centimeter / 100.0, 4)
            processed_d[ 
                self.field_dict['height'] 
            ] = meter

            self.processed_data.append(processed_d)

    def output_data_file(self, data, path=Path()):
        csv_data = [self.field_names] + data
        with path.open(mode="w") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows(csv_data)
        

if __name__ == "__main__":
    viz = Vizualization()