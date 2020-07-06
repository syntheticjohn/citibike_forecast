# Forecasting Citi Bike Demand

Time series forecasting on the daily demand of Citi Bike with a 365-day horizon for every bike station in NYC

**Project overview:**
- Forecasted Citi Bike daily demand with a 365-day horizon for each bike station in NYC using an additive regression model (Facebook Prophet) and a convolutional neural network (WaveNet)
- Preprocessed ~80 million ride data hosted on AWS S3 using Dask on GCP and trained the neural net on a GPU through Google Colab
- Prophet and WaveNet evaluated at an average MAE of 31.7 and 38.2 on the test set, respectively
- Developed and deployed an interactive Flask web app with forecasts and map navigation

**This repo includes:**

- **citibike_preprocessing.py**: data preprocessing
- **citibike_eda.ipynb**: exploratory analysis
- **citibike_modeling_fbprophet.ipynb**: Facebook Prophet training and evaluation
- **citibike_modeling_wavenet.ipynb**: WaveNet training and evaluation
- **citibike_modeling_wavenet_exog.ipynb**: WaveNet (with exogenous features) training and evaluation
- **data**: pickled files
- **app**: web app
- **citibike_forecast_slides.pdf**: pdf of project presentation slides

The web app includes an interactive time series graph with a map feature that allows the user to explore the forecasted daily demand for each Citi Bike station.

Note that additional rounds of feature extraction and modeling were completed after the presentation to improve score, so not all final features are reflected in the presentation.
