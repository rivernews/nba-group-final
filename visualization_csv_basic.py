import csv
from pathlib import Path

class CSVFileManager(object):
    def __init__(self, data_csvfilename):
        self.CSV_DATA_DIRECTORY = Path(__file__).parent / 'data'
        self.CSV_DATA_FILE_PATH = self.CSV_DATA_DIRECTORY / f'{data_csvfilename}.csv'
        
        self.CSV_DATA_OUTPUT_CLEANED_FILE_PATH = self.CSV_DATA_DIRECTORY / 'cleaned' / f'cleaned_{data_csvfilename}_output.csv'
        self.CSV_DATA_OUTPUT_INVALID_FILE_PATH = self.CSV_DATA_DIRECTORY / 'cleaned' / f'invalid_{data_csvfilename}_output.csv'
        self.CSV_DATA_OUTPUT_PROCESSED_FILE_PATH = self.CSV_DATA_DIRECTORY / 'processed' / f'processed_{data_csvfilename}.csv'

class Vizualization(object):
    def __init__(self, fields_required=None, data_csvfilename=None):
        self.csv_file_manager = CSVFileManager(data_csvfilename)
        # Store Data in Data Structure
        with self.csv_file_manager.CSV_DATA_FILE_PATH.open(mode='r') as f:
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

        # Cleaning
        if not fields_required == None:
            self.FIELDS_REQUIRED = fields_required
        else:
            self.FIELDS_REQUIRED = ['weight', 'height', 'position', 'year_start', 'year_end']
        self.clean_data()

        # Data Processing
        self.processed_data = []
        if data_csvfilename == 'player_data':
            self.convert_height_into_float_meter()
            self.augment_weight_in_kg()
            self.augment_bmi()
            self.augment_bmi_category()
            self.augment_career_duration()
        elif data_csvfilename == 'draft78':
            self.processed_data = self.cleaned_data

        # Output Results
        if len(self.processed_data) > 0:
            self.output_data_file(self.processed_data, path=self.csv_file_manager.CSV_DATA_OUTPUT_PROCESSED_FILE_PATH)
    
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
                if (
                    (not self.get_field_value(d, required_field_name))
                ):
                    valid = False
                    break
            if valid:
                self.cleaned_data.append(d)
            else:
                self.invalid_data.append(d)
        
        self.output_data_file(self.cleaned_data, path=self.csv_file_manager.CSV_DATA_OUTPUT_CLEANED_FILE_PATH)
        self.output_data_file(self.invalid_data, path=self.csv_file_manager.CSV_DATA_OUTPUT_INVALID_FILE_PATH)
    
    def convert_height_into_float_meter(self):
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

    '''
        Augment Fields
    '''
    
    def augment_field(self, target_dataset, field_name, function):
        for d in target_dataset:
            d.append(
                function(d)
            )

        index = len(self.field_names)
        self.field_names.append(field_name)
        self.field_dict.update({
            field_name: index
        })

    # Field Calculators
    #
    
    def cal_bmi(self, d):
        '''
            Refer to https://www.cdc.gov/healthyweight/assessing/bmi/childrens_bmi/childrens_bmi_formula.html
        '''
        height_in_m = d[
            self.field_dict['height']
        ]
        weight_in_kg = d[
            self.field_dict['weight-in-kg']
        ]
        return weight_in_kg / (height_in_m**2)

    def cal_bmi_category(self, d):
        '''
            Refer to https://www.nhlbi.nih.gov/health/educational/lose_wt/BMI/bmicalc.htm
        '''
        bmi = d[
            self.field_dict['bmi']
        ]
        if bmi < 18.5:
            return '0-Underweight'
        elif bmi >= 18.5 and bmi < 25:
            return '1-Normal weight'
        elif bmi >= 25 and bmi < 30:
            return '2-Overweight'
        elif bmi >= 30:
            return '3-Obesity'
    
    def cal_weight(self, d):
        lbs = float(d[ self.field_dict['weight'] ])
        return lbs / 2.205

    def cal_career_duration(self, d):
        year_start = int(d[
            self.field_dict['year_start']
        ])
        year_end = int(d[
            self.field_dict['year_end']
        ])
        return year_end - year_start + 1
    
    # Function Wrapper
    #

    def augment_weight_in_kg(self):
        self.augment_field(
            self.processed_data,
            'weight-in-kg',
            self.cal_weight
        )
    
    def augment_bmi(self):
        self.augment_field(
            self.processed_data,
            'bmi',
            self.cal_bmi
        )
    
    def augment_bmi_category(self):
        self.augment_field(
            self.processed_data,
            'bmi-category',
            self.cal_bmi_category
        )

    def augment_career_duration(self):
        self.augment_field(
            self.processed_data,
            'career-duration',
            self.cal_career_duration
        )
