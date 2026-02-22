import './style.css';
import {Map, View} from 'ol';
import TileLayer from 'ol/layer/Tile';
import VectorLayer from 'ol/layer/Vector';
import VectorSource from 'ol/source/Vector';
import OSM from 'ol/source/OSM';
import Style from 'ol/style/Style';
import Circle from 'ol/style/Circle';
import Fill from 'ol/style/Fill';
import Stroke from 'ol/style/Stroke';
import GeoJSON from 'ol/format/GeoJSON';
import Overlay from 'ol/Overlay';

const vectorSource = new VectorSource({
  url: 'http://localhost:5001/locations',
  format: new GeoJSON({
    dataProjection: 'EPSG:4326',
    featureProjection: 'EPSG:3857'
  })
});

const vectorLayer = new VectorLayer({
  source: vectorSource,
  style: new Style({
    image: new Circle({
      radius: 8,
      fill: new Fill({ color: 'red' }),
      stroke: new Stroke({ color: 'white', width: 2 })
    })
  })
});

const map = new Map({
  target: 'map',
  layers: [
    new TileLayer({ source: new OSM() }),
    vectorLayer
  ],
  view: new View({
    center: [0, 0],
    zoom: 2
  })
});

// Fits all the points on the map once the vector source is loaded
vectorSource.on('change', () => {
  if (vectorSource.getState() === 'ready' && vectorSource.getFeatures().length > 0) {
    map.getView().fit(vectorSource.getExtent(), {
      padding: [50, 50, 50, 50],
      maxZoom: 12
    });
  }
});

const popup = document.getElementById('popup');
const popupContent = document.getElementById('popup-content');
const popupClose = document.getElementById('popup-close');

const overlay = new Overlay({
  element: popup,
  positioning: 'bottom-center',
  offset: [0, -10]
});
map.addOverlay(overlay);

popupClose.addEventListener('click', () => {
  popup.classList.remove('visible');
});

map.on('click', (event) => {
  const feature = map.forEachFeatureAtPixel(event.pixel, f => f);

  if (feature) {
    const coords = feature.getGeometry().getCoordinates();
    const props = feature.getProperties();

    let html = '';

    for (const [key, value] of Object.entries(props)) {
      if (key === 'geometry' || key === 'stimulations') continue;
      html += `<p><strong>${key}:</strong> ${value}</p>`;
    }

    if (props.stimulations && props.stimulations.length > 0) {
      html += `<hr><strong>Stimulations</strong>`;
      props.stimulations.forEach((s, i) => {
        html += `
          <div style="margin-top:8px; padding:8px; background:#f5f5f5; border-radius:4px; font-size:12px;">
            <strong>Stimulation ${i + 1}</strong><br>
            Date: ${s.date_stimulated}<br>
            Formation: ${s.stimulated_formation}<br>
            Depth: ${s.top_ft} - ${s.bottom_ft} ft<br>
            Stages: ${s.stimulation_stages}<br>
            Volume: ${s.volume} ${s.volume_units}<br>
            Treatment: ${s.type_treatment}<br>
            Acid %: ${s.acid_pct}<br>
            Proppant: ${s.lbs_proppant} lbs<br>
            Max Pressure: ${s.max_treatment_pressure} psi<br>
            Max Rate: ${s.max_treatment_rate}<br>
            Details: ${s.details}
          </div>`;
      });
    }

    popupContent.innerHTML = html;
    overlay.setPosition(coords);
    popup.classList.add('visible');
  } else {
    popup.classList.remove('visible');
  }
});

map.on('pointermove', (event) => {
  const hit = map.hasFeatureAtPixel(event.pixel);
  document.getElementById('map').style.cursor = hit ? 'pointer' : '';
});
