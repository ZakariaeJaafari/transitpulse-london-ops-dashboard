"""Curated TfL StopPoints for the arrivals stream. Do not poll the whole network."""

STOPPOINTS = [
    {
        "stop_id": "940GZZLUOXC",
        "name": "Oxford Circus",
        "lines": ["bakerloo", "central", "victoria"],
    },
    {
        "stop_id": "940GZZLUKSX",
        "name": "King's Cross St. Pancras",
        "lines": ["circle", "hammersmith-city", "metropolitan", "northern", "piccadilly", "victoria"],
    },
    {
        "stop_id": "940GZZLUWLO",
        "name": "Waterloo",
        "lines": ["bakerloo", "jubilee", "northern", "waterloo-city"],
    },
    {
        "stop_id": "940GZZLUVIC",
        "name": "Victoria",
        "lines": ["circle", "district", "victoria"],
    },
]
