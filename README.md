# citibike_forecast

Demand forecasting project to forecast the daily demand of Citi Bike usage with a 365-day horizon for each bike station in NYC

This repo contains:

- **citibike_preprocessing.py**: data preprocessing
- **data**: pickled files
- **citibike_eda.ipynb**: exploratory analysis
- **citibike_modeling_fbprophet.ipynb**: Facebook Prophet training and evaluation
- **citibike_modeling_wavenet.ipynb**: WaveNet training and evaluation
- **citibike_modeling_wavenet_exog.ipynb**: WaveNet (with exogenous features) training and evaluation
- **app.py**: flask web app
- **citibike_forecast_slides.pdf**: pdf of project presentation slides

The web app is an interactive time series graph with a map feature that allows the user to explore the forecasted daily demand for each Citi Bike station.

Note that additional rounds of feature extraction and modeling were completed after the presentation to improve score, so not all final features are reflected in the presentation.
