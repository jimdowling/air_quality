import json
import time
import pickle
import joblib

import hopsworks 
import streamlit as st
from geopy import distance

import plotly.express as px
import folium
from streamlit_folium import st_folium

from functions import *



def print_fancy_header(text, font_size=22, color="#ff5f27"):
    res = f'<span style="color:{color}; font-size: {font_size}px;">{text}</span>'
    st.markdown(res, unsafe_allow_html=True)  

@st.cache_data()
def get_batch_data_from_fs(td_version, date_threshold):
    st.write(f"Retrieving the Batch data since {date_threshold}")
    feature_view.init_batch_scoring(training_dataset_version=td_version)

    batch_data = feature_view.get_batch_data(start_time=date_threshold)
    return batch_data


@st.cache_data()
def download_model(name="air_quality_xgboost_model", version=1):
    mr = project.get_model_registry()
    retrieved_model = mr.get_model(
        name="air_quality_xgboost_model",
        version=1
    )
    saved_model_dir = retrieved_model.download()
    return saved_model_dir



def plot_pm2_5(df):
    # create figure with plotly express
    fig = px.line(df, x='date', y='pm2_5', color='city_name')

    # customize line colors and styles
    fig.update_traces(mode='lines+markers')
    fig.update_layout({
        'plot_bgcolor': 'rgba(0, 0, 0, 0)',
        'paper_bgcolor': 'rgba(0, 0, 0, 0)',
        'legend_title': 'City',
        'legend_font': {'size': 12},
        'legend_bgcolor': 'rgba(0, 0, 0, 0)',
        'xaxis': {'title': 'Date'},
        'yaxis': {'title': 'PM2.5'},
        'shapes': [{
            'type': 'line',
            'x0': datetime.datetime.now().strftime('%Y-%m-%d'),
            'y0': 0,
            'x1': datetime.datetime.now().strftime('%Y-%m-%d'),
            'y1': df['pm2_5'].max(),
            'line': {'color': 'red', 'width': 2, 'dash': 'dashdot'}
        }]
    })

    # show plot
    st.plotly_chart(fig, use_container_width=True)


with open('target_cities.json') as json_file:
    target_cities = json.load(json_file)


#########################
st.title('🌫 Air Quality Prediction 🌦')

st.write(3 * "-")
print_fancy_header('\n📡 Connecting to Hopsworks Feature Store...')

st.write("Logging... ")
# (Attention! If the app has stopped at this step,
# please enter your Hopsworks API Key in the commmand prompt.)
project = hopsworks.login()
fs = project.get_feature_store()
st.write("✅ Logged in successfully!")

st.write("Getting the Feature View...")
feature_view = fs.get_feature_view(
    name = 'air_quality_fv',
    version = 1
)
st.write("✅ Success!")

# I am going to load data for of last 60 days (for feature engineering)
today = datetime.date.today()
date_threshold = today - datetime.timedelta(days=60)

st.write(3 * "-")
print_fancy_header('\n☁️ Retriving batch data from Feature Store...')
batch_data = get_batch_data_from_fs(td_version=1,
                                    date_threshold=date_threshold)

st.write("Batch data:")
st.write(batch_data.sample(5))            


saved_model_dir = download_model(
    name="air_quality_xgboost_model",
    version=1
)

pipeline = joblib.load(saved_model_dir + "/xgboost_pipeline.pkl")
st.write("\n")
st.write("✅ Model was downloaded and cached.")

st.write(3 * '-')
st.write("\n")
print_fancy_header(text="🖍 Select the cities using the form below. \
                         Click the 'Submit' button at the bottom of the form to continue.",
                   font_size=22)
dict_for_streamlit = {}
for continent in target_cities:
        for city_name, coords in target_cities[continent].items():
            dict_for_streamlit[city_name] = coords
selected_cities_full_list = []

with st.form(key="user_inputs"):
    print_fancy_header(text='\n🗺 Here you can choose cities from the drop-down menu',
                       font_size=20, color="#00FFFF")
    
    cities_multiselect = st.multiselect(label='',
                                        options=dict_for_streamlit.keys())
    selected_cities_full_list.extend(cities_multiselect)
    st.write("_" * 3)
    print_fancy_header(text="\n📌 To add a city using the interactive map, click somewhere \
                             (for the coordinates to appear)",
                       font_size=20, color="#00FFFF")
    
    my_map = folium.Map(location=[42.57, -44.092], zoom_start=2)
    # Add markers for each city
    for city_name, coords in dict_for_streamlit.items():
        folium.CircleMarker(
            location=coords
        ).add_to(my_map)

    my_map.add_child(folium.LatLngPopup())
    res_map = st_folium(my_map, width=640, height=480)
    
    try:
        new_lat, new_long = res_map["last_clicked"]["lat"], res_map["last_clicked"]["lng"]

        # Calculate the distance between the clicked location and each city
        distances = {city: distance.distance(coord, (new_lat, new_long)).km for city, coord in dict_for_streamlit.items()}

        # Find the city with the minimum distance and print its name
        nearest_city = min(distances, key=distances.get)
        print_fancy_header(text=f"You have selected {nearest_city} using map", font_size=18, color="#52fa23")
        
        selected_cities_full_list.append(nearest_city)
        st.write(label_encoder.transform([nearest_city])[0])

    except Exception as err:
        print(err)
        pass
    
    submit_button = st.form_submit_button(label='Submit')

if submit_button:
    st.write('Selected cities:', selected_cities_full_list)

    st.write(3*'-')
   
    dataset = batch_data
  
    dataset = dataset.sort_values(by=["city_name", "date"])

    st.write("\n")
    print_fancy_header(text='\n🧠 Predicting PM2.5 for selected cities...',
                       font_size=18, color="#FDF4F5")
    st.write("")
    preds = pd.DataFrame(columns=dataset.columns)
    for city_name in selected_cities_full_list:
        st.write(f"\t * {city_name}...")
        features = dataset.loc[dataset['city_name'] == city_name]
        print(features.head())
        features['pm2_5'] = pipeline.predict(features)
        preds = pd.concat([preds, features])
    
    st.write("")
    print_fancy_header(text="📈Results 📉",
                       font_size=22)
    plot_pm2_5(preds[preds['city_name'].isin(selected_cities_full_list)])

    st.write(3 * "-")
    st.subheader('\n🎉 📈 🤝 App Finished Successfully 🤝 📈 🎉')
    st.button("Re-run")
