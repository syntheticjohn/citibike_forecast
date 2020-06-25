# citibike_forecast

Demand forecasting project to forecast the daily demand of Citi Bike usage with a 365-day horizon for each bike station in NYC

This repo contains:

- citibike_preprocessing.py: data preprocessing
- citibike_preprocessing.ipynb: data preprocessing
- citibike_eda.ipynb: exploratory analysis
- citibike_modeling.ipynb: Facebook Prophet training, WaveNet training and prediction output
- forecast_app.ipynb: forecast visuals for app
- app.py: flask web app
- citibike_forecast_slides.pdf: pdf of project presentation slides

The web app is an interactive time series graph with a map feature that allows the user to explore the forecasted daily demand for each Citi Bike station.

Note that I did more feature extraction and another pass at modeling after the presentation to improve my competition score, so not all of my final features are reflected in the presentation.
