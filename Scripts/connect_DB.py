from flask import Flask, jsonify
from flask_cors import CORS
import duckdb
import json

app = Flask(__name__)
CORS(app, origins=["http://localhost"])

db = duckdb.connect('mydb.duckdb', read_only=True)
db.execute("LOAD spatial")

@app.route("/locations")
def locations():
    cursor = db.execute("""
        SELECT API, well_name, well_number, well_status, well_type,
               address, operator, state, closest_city, oil_bbl, gas_mcf,
               ST_AsGeoJSON(geom), stimulations
        FROM locations
    """)

    features = []
    for row in cursor.fetchall():
        api, name, number, status, well_type, address, operator, state, city, oil, gas, geom_json, stimulations = row
        features.append({
            "type": "Feature",
            "geometry": json.loads(geom_json),
            "properties": {
                "API": api,
                "Well Name": name,
                "Well Number": number,
                "Status": status,
                "Type": well_type,
                "Address": address,
                "Operator": operator,
                "State": state,
                "Closest City": city,
                "Oil (bbl)": oil,
                "Gas (mcf)": gas,
                "stimulations": stimulations
            }
        })

    return jsonify({
        "type": "FeatureCollection",
        "features": features
    })

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False, port=5001)
