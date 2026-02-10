# MultiViz Time Waveform Upload Example

This repository contains a small Python project and a Jupyter notebook showing how to upload **time waveform vibration data** to **MultiViz** using the `MultivizClient`.  
The notebook demonstrates working with both **JSON payloads** and **CSV files**.

---

## 📂 Project Structure

```
.
├── data/
│   ├── information.txt        # Metadata for CSV example
│   ├── sample_payload.json    # JSON waveform example
│   ├── values_1.csv           # X-axis data
│   ├── values_2.csv           # Y-axis data
│   └── values_3.csv           # Z-axis data
├── notebooks/
│   └── upload_to_multiviz.ipynb # Main demonstration notebook
├── src/
│   ├── helper.py              # Helper functions (I/O, parsing, utils)
│   ├── logger.py              # Basic logger
│   ├── multiviz_client.py     # MultiViz communication layer
├── requirements.txt
└── README.md
```

---

## 🚀 Setup Instructions

### 1. Create & activate a virtual environment

**Windows:**
```bash
python -m venv .venv
.\.venv\Scripts\activate
```

**macOS / Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

---

### 2. Install dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

If you plan to run the notebook and Jupyter is not included:
```bash
pip install jupyterlab
```

---

## 🔑 Configure MultiViz API Key

Open the notebook:

```
notebooks/upload_to_multiviz.ipynb
```

Set your API key in the first code cell:

```python
MULTIVIZ_API_KEY = "YOUR_API_KEY_HERE"
MULTIVIZ_BASE_URL = "https://api.beta.multiviz.com"
```

---

## 📘 Running the Notebook

From the project root:

```bash
jupyter lab
```

Then open:

```
notebooks/upload_to_multiviz.ipynb
```

Run all cells from top to bottom.

The notebook will:

- Initialize the `MultivizClient`
- Show how to create waveform **sources**
- Upload waveform **measurements** from JSON or CSV

---

## 📑 Example 1 — JSON Upload

This example uses:

```
data/sample_payload.json
```

The function `upload_example_measurement_json()`:

- Loads payload  
- Builds waveform sources  
- Uploads waveform samples (X/Y/Z axes)  
- Sends metadata (location, asset, sensor, gateway, process_data, etc.)

To run:

```python
upload_example_measurement_json()
```

---

## 📑 Example 2 — CSV Upload

Files:

```
data/information.txt
data/values_1.csv
data/values_2.csv
data/values_3.csv
```

`information.txt` includes metadata such as:

```
Snapshot Id     : 202804
Recorded At     : 10/22/2025 00:58:37
Recorded by User: Unknown
Device Name        : UT-CMP-201
Device Serial      : VW8AQ5A840
Machine Name       : cylinder
Sensor Serial      : 1890727266
Sensor Name        : Sensor 1890727266
Samples            : 8192
Time Period        : 640ms
Sensor Position    : 1
Axis               : All
Freq Max           : 5000
FFT Res            : 1.563
Avg. Number        : 1
Slice/Bin Duration : 0ms
```

The function `upload_example_measurement_csv()` will:

- Parse metadata
- Load CSV sample columns
- Create waveform sources (X/Y/Z)
- Upload each measurement

Run:

```python
upload_example_measurement_csv()
```

---

## 🛠 Troubleshooting

### Import errors:
If you get:
```
ModuleNotFoundError: No module named 'src'
```

Ensure you launched Jupyter from the **project root**.

Or manually add it:

```python
import sys
sys.path.append(".")
```

---

## 📬 Support

If anything is unclear or you want the README adapted for internal documentation, just let me know!


## 📦 Download

[📥 Download ZIP (v1.0.2)](https://github.com/vikinganalytics/multiviz-api-getting-started/releases/download/v1.0.2/multiviz-api-getting-started-v1.0.2.zip)

Or always get the newest version:

[⭐ Latest release](https://github.com/vikinganalytics/multiviz-api-getting-started/releases/latest)

