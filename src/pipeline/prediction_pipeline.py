import sys 
import os 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from exception import CustomException 
from logger import logging 
from utils import load_obj
import pandas as pd

class PredictPipeline: 
    def __init__(self) -> None:
        pass

    def predict(self, features): 
        try: 
           
            preprocessor_path = os.path.join('dataset', 'preprocessor.pkl')
            model_path = os.path.join('dataset', 'model.pkl')

            preprocessor_dict = load_obj(preprocessor_path)
            model = load_obj(model_path)

            scaler = preprocessor_dict['scaler']
            encoders = preprocessor_dict['encoders']
            
            # Encode categorical variables (ensure lowercase)
            features_encoded = features.copy()
            for col in ['State_Name', 'Crop_Type', 'Crop']:
                if col in features_encoded.columns:
                    # Convert to lowercase to match training data
                    features_encoded[col] = features_encoded[col].str.lower()
                    features_encoded[col] = encoders[col].transform(features_encoded[col])
            
            data_scaled = scaler.transform(features_encoded)
            pred = model.predict(data_scaled)
            return pred
        except Exception as e: 
            logging.info("Error occured in predict function in prediction_pipeline location")
            raise CustomException(e,sys)
        
class CustomData: 
        def __init__(self, N:float, 
                     P:float, 
                     K:float, 
                     pH:float, 
                     rainfall:float, 
                     temperature:float, 
                     Area_in_hectares:float,
                     State_Name:str, 
                     Crop_Type:str, 
                     Crop:str): 
             self.N = N
             self.P = P
             self.K = K
             self.pH = pH
             self.rainfall = rainfall 
             self.temperature = temperature
             self.Area_in_hectares = Area_in_hectares
             self.State_Name = State_Name 
             self.Crop_Type = Crop_Type 
             self.Crop = Crop
        
        def get_data_as_dataframe(self): 
             try: 
                  custom_data_input_dict = {
                       'N': [self.N], 
                       'P': [self.P], 
                       'K': [self.K], 
                       'pH': [self.pH],
                       'rainfall':[self.rainfall],
                       'temperature':[self.temperature], 
                       'Area_in_hectares':[self.Area_in_hectares],
                       'State_Name': [self.State_Name], 
                       'Crop_Type': [self.Crop_Type], 
                       'Crop': [self.Crop]

                  }
                  df = pd.DataFrame(custom_data_input_dict)
                  logging.info("Dataframe created")
                  return df
             except Exception as e:
                  logging.info("Error occured in get_data_as_dataframe function in prediction_pipeline")
                  raise CustomException(e,sys) 
             
             
        