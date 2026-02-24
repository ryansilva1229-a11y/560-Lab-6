from flask import Flask, jsonify
from flask_cors import CORS
import duckdb
import json

app = Flask(__name__)
CORS(app, origins=["http://localhost"])

db = duckdb.connect('mydb.duckdb')
db.execute("LOAD spatial")
db.execute("""
    ALTER TABLE OIL_DATA
    ADD COLUMN geom GEOMETRY
""")
db.execute("""
    UPDATE OIL_DATA
    SET geom = ST_Point(
        TRY_CAST(longitude AS DOUBLE),
        TRY_CAST(latitude AS DOUBLE)
    )
    WHERE TRY_CAST(longitude AS DOUBLE) IS NOT NULL
      AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
""")

@app.route("/locations")
def get_locations():
    cursor = db.execute("""
        SELECT
            api_number,
            well_name,
            operator,
            job_number,
            job_type,
            county,
            state,
            shl,
            well_status,
            well_type,
            closest_city,
            oil_bbl,
            gas_mcf,
            source_pdf,
            stimulation,
            ST_AsGeoJSON(geom) AS geom_json
        FROM OIL_DATA
        WHERE geom IS NOT NULL
            AND latitude IS NOT NULL
            AND longitude IS NOT NULL
    """)

    features = []

    for row in cursor.fetchall():
        (
            api_number,
            well_name,
            operator,
            job_number,
            job_type,
            county,
            state,
            shl,
            well_status,
            well_type,
            closest_city,
            oil_bbl,
            gas_mcf,
            source_pdf,
            stimulation,
            geom_json
        ) = row

        try:
            stimulation_data = json.loads(stimulation) if stimulation else []
        except json.JSONDecodeError:
            stimulation_data = []

        features.append({
            "type": "Feature",
            "geometry": json.loads(geom_json) if geom_json else None,
            "properties": {
                "api_number": api_number,
                "well_name": well_name,
                "operator": operator,
                "job_number": job_number,
                "job_type": job_type,
                "county": county,
                "state": state,
                "shl": shl,
                "well_status": well_status,
                "well_type": well_type,
                "closest_city": closest_city,
                "oil_bbl": oil_bbl,
                "gas_mcf": gas_mcf,
                "source_pdf": source_pdf,
                "stimulation": stimulation_data
            }
        })

    return jsonify({
        "type": "FeatureCollection",
        "features": features
    })

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False, port=5001)
